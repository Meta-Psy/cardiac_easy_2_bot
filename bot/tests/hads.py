"""
HADS - Госпитальная шкала тревоги и депрессии
"""
from typing import Dict, List, Tuple

def get_hads_questions() -> List[Dict]:
    """HADS - Госпитальная шкала тревоги и депрессии"""
    return [
        {
            "id": 1,
            "text": "Я испытываю напряженность, мне не по себе",
            "type": "anxiety",
            "options": [
                {"text": "всё время", "score": 3},
                {"text": "часто", "score": 2},
                {"text": "время от времени, иногда", "score": 1},
                {"text": "совсем не испытываю", "score": 0}
            ]
        },
        {
            "id": 2,
            "text": "То, что приносило мне большое удовольствие, и сейчас вызывает у меня такое же чувство",
            "type": "depression",
            "options": [
                {"text": "определенно, это так", "score": 0},
                {"text": "наверное, это так", "score": 1},
                {"text": "лишь в очень малой степени это так", "score": 2},
                {"text": "это совсем не так", "score": 3}
            ]
        },
        {
            "id": 3,
            "text": "Я испытываю страх, кажется, будто что-то ужасное может вот-вот случиться",
            "type": "anxiety",
            "options": [
                {"text": "определенно, это так, и страх очень велик", "score": 3},
                {"text": "да, это так, но страх не очень сильный", "score": 2},
                {"text": "иногда, но это меня не беспокоит", "score": 1},
                {"text": "совсем не испытываю", "score": 0}
            ]
        },
        {
            "id": 4,
            "text": "Я способен рассмеяться и увидеть в том или ином событии смешное",
            "type": "depression",
            "options": [
                {"text": "определенно, это так", "score": 0},
                {"text": "наверное, это так", "score": 1},
                {"text": "лишь в очень малой степени это так", "score": 2},
                {"text": "это совсем не так", "score": 3}
            ]
        },
        {
            "id": 5,
            "text": "Беспокойные мысли крутятся у меня в голове",
            "type": "anxiety",
            "options": [
                {"text": "постоянно", "score": 3},
                {"text": "большую часть времени", "score": 2},
                {"text": "время от времени и не так часто", "score": 1},
                {"text": "только иногда", "score": 0}
            ]
        },
        {
            "id": 6,
            "text": "Я испытываю бодрость",
            "type": "depression",
            "options": [
                {"text": "совсем не испытываю", "score": 3},
                {"text": "очень редко", "score": 2},
                {"text": "иногда", "score": 1},
                {"text": "практически всё время", "score": 0}
            ]
        },
        {
            "id": 7,
            "text": "Я легко могу сесть и расслабиться",
            "type": "anxiety",
            "options": [
                {"text": "определенно, это так", "score": 0},
                {"text": "наверно, это так", "score": 1},
                {"text": "лишь изредка это так", "score": 2},
                {"text": "совсем не могу", "score": 3}
            ]
        },
        {
            "id": 8,
            "text": "Мне кажется, что я всё стал делать очень медленно",
            "type": "depression",
            "options": [
                {"text": "практически всё время", "score": 3},
                {"text": "часто", "score": 2},
                {"text": "иногда", "score": 1},
                {"text": "совсем нет", "score": 0}
            ]
        },
        {
            "id": 9,
            "text": "Я испытываю внутреннее напряжение или дрожь",
            "type": "anxiety",
            "options": [
                {"text": "совсем не испытываю", "score": 0},
                {"text": "иногда", "score": 1},
                {"text": "часто", "score": 2},
                {"text": "очень часто", "score": 3}
            ]
        },
        {
            "id": 10,
            "text": "Я не слежу за своей внешностью",
            "type": "depression",
            "options": [
                {"text": "определенно, это так", "score": 3},
                {"text": "я не уделяю этому столько времени, сколько нужно", "score": 2},
                {"text": "может быть, я стал меньше уделять этому внимания", "score": 1},
                {"text": "я слежу за собой так же, как и раньше", "score": 0}
            ]
        },
        {
            "id": 11,
            "text": "Я испытываю неусидчивость, словно мне постоянно нужно двигаться",
            "type": "anxiety",
            "options": [
                {"text": "определенно, это так", "score": 3},
                {"text": "наверно, это так", "score": 2},
                {"text": "лишь в некоторой степени это так", "score": 1},
                {"text": "совсем не испытываю", "score": 0}
            ]
        },
        {
            "id": 12,
            "text": "Я считаю, что мои дела (занятия, увлечения) могут принести мне чувство удовлетворения",
            "type": "depression",
            "options": [
                {"text": "точно так же, как и обычно", "score": 0},
                {"text": "да, но не в той степени, как раньше", "score": 1},
                {"text": "значительно меньше, чем обычно", "score": 2},
                {"text": "совсем так не считаю", "score": 3}
            ]
        },
        {
            "id": 13,
            "text": "У меня бывает внезапное чувство паники",
            "type": "anxiety",
            "options": [
                {"text": "очень часто", "score": 3},
                {"text": "довольно часто", "score": 2},
                {"text": "не так уж часто", "score": 1},
                {"text": "совсем не бывает", "score": 0}
            ]
        },
        {
            "id": 14,
            "text": "Я могу получить удовольствие от хорошей книги, радио- или телепрограммы",
            "type": "depression",
            "options": [
                {"text": "часто", "score": 0},
                {"text": "иногда", "score": 1},
                {"text": "редко", "score": 2},
                {"text": "очень редко", "score": 3}
            ]
        }
    ]

def calculate_hads_scores(answers: List[int]) -> Tuple[int, int]:
    """Рассчитывает баллы тревоги и депрессии для HADS"""
    questions = get_hads_questions()
    anxiety_score = 0
    depression_score = 0
    
    for i, answer in enumerate(answers):
        if i < len(questions):
            if questions[i]["type"] == "anxiety":
                anxiety_score += answer
            else:
                depression_score += answer
    
    return anxiety_score, depression_score

def get_hads_interpretation(anxiety_score: int, depression_score: int) -> str:
    """Интерпретация результатов HADS"""
    def get_level(score: int) -> str:
        if score <= 7:
            return "норма"
        elif score <= 10:
            return "субклинически выраженные симптомы"
        else:
            return "клинически выраженные симптомы"
    
    anxiety_level = get_level(anxiety_score)
    depression_level = get_level(depression_score)
    
    result = f"<b>Тревога:</b> {anxiety_score} баллов - {anxiety_level}\n"
    result += f"<b>Депрессия:</b> {depression_score} баллов - {depression_level}\n\n"
    
    if anxiety_score > 10 or depression_score > 10:
        result += "🚨 <b>Рекомендация:</b> Рекомендуется консультация специалиста (психолога, психотерапевта) для дальнейшей оценки и возможной помощи."
    elif anxiety_score > 7 or depression_score > 7:
        result += "⚠️ <b>Рекомендация:</b> Обратите внимание на свое эмоциональное состояние. Рассмотрите возможность работы со стрессом, релаксационные техники."
    else:
        result += "✅ <b>Результат:</b> Ваши показатели тревоги и депрессии находятся в пределах нормы."
    
    return result