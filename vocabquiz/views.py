import random
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from .vocab import VOCAB
from .databricks_quiz import DATABRICKS_QUIZ


def quiz_page(request):
    return render(request, "vocabquiz/quiz.html")


# ---------------------------
# VOCAB (igual que lo tienes)
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
# DATABRICKS (nuevo: exam=1|2|3|all)
# ---------------------------
def _normalize_exam(raw: str) -> str:
    raw = (raw or "all").strip().lower()
    if raw in {"1", "2", "3"}:
        return raw
    return "all"


def _get_dbx_questions_for_exam(exam: str):
    # exam viene normalizado: "1"/"2"/"3"/"all"
    if exam == "all":
        return DATABRICKS_QUIZ
    ex_num = int(exam)
    return [q for q in DATABRICKS_QUIZ if int(q.get("exam", 0)) == ex_num]


def _get_dbx_pool(request, exam: str):
    key = f"seen_dbx_exam_{exam}"  # exam = 1/2/3/all
    seen = set(request.session.get(key, []))
    questions = _get_dbx_questions_for_exam(exam)
    remaining = [i for i in range(len(questions)) if i not in seen]
    return key, seen, remaining, questions


@require_http_methods(["GET"])
def dbx_next(request):
    exam = _normalize_exam(request.GET.get("exam", "all"))
    key, seen, remaining, questions = _get_dbx_pool(request, exam)

    if not remaining:
        return JsonResponse({"done": True, "exam": exam, "total": len(questions)})

    idx = random.choice(remaining)
    q = questions[idx]

    seen.add(idx)
    request.session[key] = list(seen)

    options = list(q["options"])
    random.shuffle(options)

    return JsonResponse({
        "done": False,
        "type": "dbx-mcq",
        "exam": exam,
        "id": q.get("id", str(idx)),
        "question": q["question"],
        "options": options,
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
    picked = (data.get("picked") or "").strip()

    # Buscar por id en TODO el banco (da igual el examen)
    q = next((x for x in DATABRICKS_QUIZ if x.get("id") == qid), None)
    if q is None:
        return JsonResponse({"ok": False, "error": "Question not found"}, status=404)

    correct = q["answer"]
    ok = (picked == correct)

    return JsonResponse({
        "ok": ok,
        "picked": picked,
        "correct": correct,
        "explanation": q.get("explanation", ""),
    })


@require_http_methods(["GET"])
def dbx_reset(request):
    """
    /api/dbx/reset/?exam=1|2|3|all
    - Si exam=all => borra progreso de todos los exámenes (incluido all)
    - Si exam=1 => borra solo ese
    """
    exam = _normalize_exam(request.GET.get("exam", "all"))

    if exam == "all":
        for k in list(request.session.keys()):
            if k.startswith("seen_dbx_exam_"):
                del request.session[k]
        return JsonResponse({"ok": True, "exam": "all"})

    key = f"seen_dbx_exam_{exam}"
    if key in request.session:
        del request.session[key]
    return JsonResponse({"ok": True, "exam": exam})