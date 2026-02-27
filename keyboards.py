from aiogram.types import KeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, InlineKeyboardButton
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


hll = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Купить звезды✨", callback_data="buy_stars")],
    [InlineKeyboardButton(text='Telegram Premium💙', callback_data="premium")],
    [InlineKeyboardButton(text="Реферальная система👥", callback_data="ref_system")],
    [InlineKeyboardButton(text="🏆 Лидеры", callback_data="leaders")],
    [
        InlineKeyboardButton(text="Помощь❓", callback_data="help"),
        InlineKeyboardButton(text="Отзывы📩", url="https://t.me/+Qkb-Q43fRf40NGFk")
    ]
])



buy_with_promo = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="🎟 Ввести промо-код", callback_data="enter_promo")
        ],
        [
            InlineKeyboardButton(text="💳 Купить СБП", callback_data="pay_sbp")
        ],
        [
            InlineKeyboardButton(text="⬅️Назад", callback_data="back")
        ]
    ]
)

buy_final = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="💳 Купить СБП", callback_data="pay_sbp")
        ],
        [
            InlineKeyboardButton(text="⬅️Назад", callback_data="back")
        ]
    ]
)


sublim = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Подать заявку", callback_data='submit_application')]
    ]
)
help = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Задать вопрос", url='https://t.me/SupTGStars')],
        [InlineKeyboardButton(text="⬅️Назад", callback_data='back_first')]
    ]
)


prem = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="💙3 месяца | 1190₽💙", callback_data='1190')],
        [InlineKeyboardButton(text="💙6 месяцев | 1490₽💙", callback_data='1490')],
        [InlineKeyboardButton(text="💙12 месяцев | 2550₽💙", callback_data='2690')],
[InlineKeyboardButton(text="⬅️Назад", callback_data="back_start")]
    ]
)


buyprem = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="💳 Купить СБП", callback_data="pay_prem")
                ],
                [
                    InlineKeyboardButton(text="⬅️Назад", callback_data="back_prem")
                ]
            ]
        )


buy_prem_with_promo = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="🎟 Ввести промо-код", callback_data="enter_promo_prem")
        ],
        [
            InlineKeyboardButton(text="💳 Купить СБП", callback_data="pay_prem")
        ],
        [
            InlineKeyboardButton(text="⬅️Назад", callback_data="back_prem")
        ]
    ]
)
