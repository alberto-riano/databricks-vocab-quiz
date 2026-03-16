import random
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from .vocab import VOCAB

# ---------------------------
# DATABRICKS ASSOCIATE
# ---------------------------
from .databricks_quiz import DATABRICKS_QUIZ as DATABRICKS_ASSOCIATE_QUIZ

try:
    from .databricks_quiz_es import DATABRICKS_QUIZ as DATABRICKS_ASSOCIATE_QUIZ_ES
except Exception:
    DATABRICKS_ASSOCIATE_QUIZ_ES = []

# ---------------------------
# DATABRICKS PROFESSIONAL
# ---------------------------
from .databricks_professional_quiz import (
    DATABRICKS_PROFESSIONAL_QUIZ as DATABRICKS_PROFESSIONAL_QUIZ
)

try:
    from .databricks_professional_quiz_es import (
        DATABRICKS_PROFESSIONAL_QUIZ as DATABRICKS_PROFESSIONAL_QUIZ_ES
    )
except Exception:
    DATABRICKS_PROFESSIONAL_QUIZ_ES = []


def quiz_databricks_page(request):
    return render(request, "vocabquiz/quiz_databricks.html")


def quiz_english_page(request):
    return render(request, "vocabquiz/quiz_english.html")


# ---------------------------
# VOCAB
# ---------------------------
def _get_pool(request, mode: str):
    key = f"seen_{mode}"
    seen = set(request.session.get(key, []))
    all_idx = list(range(len(VOCAB)))
    remaining = [i for i in all_idx if i not in seen]
    return key, seen, remaining


@require_http_methods(["GET"])
def reset_game(request):
    for k in list(request.session.keys()):
        if k.startswith("seen_"):
            del request.session[k]
    return JsonResponse({"ok": True})


@require_http_methods(["GET"])
def next_question(request):
    mode = request.GET.get("mode", "es-en")
    if mode not in ("es-en", "en-es", "es-en-write"):
        mode = "es-en"

    key, seen, remaining = _get_pool(request, mode)

    if not remaining:
        return JsonResponse({"done": True, "mode": mode, "total": len(VOCAB)})

    idx = random.choice(remaining)
    en, es = VOCAB[idx]

    seen.add(idx)
    request.session[key] = list(seen)

    if mode == "es-en":
        prompt = es
        correct = en
        pool = [x[0] for x in VOCAB if x[0] != correct]
        distractors = random.sample(pool, k=3) if len(pool) >= 3 else pool
        options = distractors + [correct]
        random.shuffle(options)
        return JsonResponse({
            "done": False,
            "type": "mcq",
            "mode": mode,
            "prompt": prompt,
            "options": options,
            "answer": correct,
            "progress": {"seen": len(seen), "total": len(VOCAB)},
        })

    if mode == "en-es":
        prompt = en
        correct = es
        pool = [x[1] for x in VOCAB if x[1] != correct]
        distractors = random.sample(pool, k=3) if len(pool) >= 3 else pool
        options = distractors + [correct]
        random.shuffle(options)
        return JsonResponse({
            "done": False,
            "type": "mcq",
            "mode": mode,
            "prompt": prompt,
            "options": options,
            "answer": correct,
            "progress": {"seen": len(seen), "total": len(VOCAB)},
        })

    return JsonResponse({
        "done": False,
        "type": "write",
        "mode": mode,
        "prompt": es,
        "answer": en,
        "progress": {"seen": len(seen), "total": len(VOCAB)},
    })


@csrf_exempt
@require_http_methods(["POST"])
def check_answer(request):
    import json
    try:
        data = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"ok": False, "error": "Invalid JSON"}, status=400)

    expected = (data.get("expected") or "").strip()
    typed = (data.get("typed") or "").strip()

    def norm(s: str) -> str:
        return " ".join(s.lower().split())

    ok = norm(expected) == norm(typed)
    return JsonResponse({"ok": ok, "expected": expected, "typed": typed})


# ---------------------------
# DATABRICKS
# track=associate|professional
# exam=1|2|3|4|5|all|1es|2es|3es|4es|5es|alles
# ---------------------------
def _normalize_track(raw: str) -> str:
    raw = (raw or "associate").strip().lower()
    allowed = {"associate", "professional"}
    return raw if raw in allowed else "associate"


def _normalize_exam(raw: str) -> str:
    raw = (raw or "all").strip().lower()
    allowed = {"1", "2", "3", "4", "5", "all", "1es", "2es", "3es", "4es", "5es", "alles"}
    return raw if raw in allowed else "all"


def _is_es(exam: str) -> bool:
    return exam.endswith("es") or exam == "alles"


def _base_exam_number(exam: str):
    """
    Devuelve 1/2/3/4/5 o None si exam es all/alles
    """
    if exam in {"all", "alles"}:
        return None
    return int(exam.replace("es", ""))


def _get_dbx_bank(track: str, exam: str):
    """
    Devuelve el banco correcto según:
    - track: associate | professional
    - idioma: deducido desde exam
    """
    is_es = _is_es(exam)

    if track == "professional":
        if is_es and DATABRICKS_PROFESSIONAL_QUIZ_ES:
            return DATABRICKS_PROFESSIONAL_QUIZ_ES
        return DATABRICKS_PROFESSIONAL_QUIZ

    if is_es and DATABRICKS_ASSOCIATE_QUIZ_ES:
        return DATABRICKS_ASSOCIATE_QUIZ_ES
    return DATABRICKS_ASSOCIATE_QUIZ


def _get_dbx_questions_for_exam(track: str, exam: str):
    bank = _get_dbx_bank(track, exam)
    ex_num = _base_exam_number(exam)

    if ex_num is None:
        return bank

    return [q for q in bank if int(q.get("exam", 0)) == ex_num]


def _get_dbx_pool(request, track: str, exam: str):
    key = f"seen_dbx_{track}_exam_{exam}"
    seen = set(request.session.get(key, []))
    questions = _get_dbx_questions_for_exam(track, exam)
    remaining = [i for i in range(len(questions)) if i not in seen]
    return key, seen, remaining, questions


def _pick_text(q: dict, is_es: bool):
    """
    Devuelve (question, explanation) usando:
      - ES: question_es/explanation_es (fallback a EN)
      - EN: question/explanation (fallback a ES)
    """
    if is_es:
        question = q.get("question_es") or q.get("question") or ""
        explanation = q.get("explanation_es") or q.get("explanation") or ""
        return question, explanation

    question = q.get("question") or q.get("question_es") or ""
    explanation = q.get("explanation") or q.get("explanation_es") or ""
    return question, explanation


@require_http_methods(["GET"])
def dbx_next(request):
    track = _normalize_track(request.GET.get("track", "associate"))
    exam = _normalize_exam(request.GET.get("exam", "all"))
    key, seen, remaining, questions = _get_dbx_pool(request, track, exam)

    if not remaining:
        return JsonResponse({
            "done": True,
            "track": track,
            "exam": exam,
            "total": len(questions)
        })

    idx = random.choice(remaining)
    q = questions[idx]

    seen.add(idx)
    request.session[key] = list(seen)

    options = list(q.get("options", []))
    random.shuffle(options)

    is_es = _is_es(exam)
    question, _ = _pick_text(q, is_es)
    is_multi = isinstance(q.get("answer"), list)

    return JsonResponse({
        "done": False,
        "type": "dbx-mcq",
        "track": track,
        "exam": exam,
        "id": q.get("id", str(idx)),
        "question": question,
        "options": options,
        "is_multi": is_multi,
        "progress": {"seen": len(seen), "total": len(questions)},
    })


@csrf_exempt
@require_http_methods(["POST"])
def dbx_check(request):
    import json
    try:
        data = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"ok": False, "error": "Invalid JSON"}, status=400)

    qid = (data.get("id") or "").strip()
    track = _normalize_track(data.get("track") or "associate")
    exam = _normalize_exam(data.get("exam") or "all")
    picked = data.get("picked")

    bank = _get_dbx_bank(track, exam)
    q = next((x for x in bank if x.get("id") == qid), None)

    if q is None:
        if track == "professional":
            q = next((x for x in DATABRICKS_PROFESSIONAL_QUIZ if x.get("id") == qid), None)
        else:
            q = next((x for x in DATABRICKS_ASSOCIATE_QUIZ if x.get("id") == qid), None)

    if q is None:
        return JsonResponse({"ok": False, "error": "Question not found"}, status=404)

    correct = q.get("answer")

    if isinstance(correct, list):
        picked_set = set(picked or [])
        correct_set = set(correct)
        ok = picked_set == correct_set
        multi = True
    else:
        picked_str = (picked or "").strip()
        ok = (picked_str == correct)
        multi = False

    is_es = _is_es(exam)
    _, explanation = _pick_text(q, is_es)

    return JsonResponse({
        "ok": ok,
        "picked": picked,
        "correct": correct,
        "multi": multi,
        "explanation": explanation,
    })


@require_http_methods(["GET"])
def dbx_reset(request):
    """
    /api/dbx/reset/?track=associate|professional&exam=1|2|3|4|5|all|1es|2es|3es|4es|5es|alles
    """
    track = _normalize_track(request.GET.get("track", "associate"))
    exam = _normalize_exam(request.GET.get("exam", "all"))

    if exam in {"all", "alles"}:
        prefix = f"seen_dbx_{track}_exam_"
        for k in list(request.session.keys()):
            if k.startswith(prefix):
                del request.session[k]
        return JsonResponse({"ok": True, "track": track, "exam": exam})

    key = f"seen_dbx_{track}_exam_{exam}"
    if key in request.session:
        del request.session[key]

    return JsonResponse({"ok": True, "track": track, "exam": exam})