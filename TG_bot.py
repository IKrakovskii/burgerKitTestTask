import asyncio
import aiogram
from loguru import logger
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

from Database_metods import DB
from Tokens_and_passwords.tokens_and_passwords import token
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

ADMIN_TG_ID = '351162658'

logger.add(
    'logs/debug.log',
    format='{time} {level} {message}',
    level='DEBUG'
)
logger.add(
    'logs/errors.log',
    format='{time} {level} {message}',
    level='WARNING'
)

bot = Bot(token=token)

# Создаем объект диспетчера
dp = Dispatcher(bot)

first_id = '351162658'
second_id = '1269197191'
db = DB()


# logger.debug('бот запущен успешно')


@logger.catch
async def on_startup(_):
    logger.info('Бот запущен')
    logger.info('Парсер гугл таблиц и БД запущены')
    from get_info_from_google_sheet import parsing_tasks
    asyncio.create_task(parsing_tasks())


@logger.catch
def logging_input_message(message: types.Message) -> None:
    logger.debug(
        f'\nВходящее сообщение от пользователя '
        f'{message.from_user.first_name}'
        f'\n\t{message.text}\n\tid пользователя:{message.from_user.id}'
    )


# Обработчик команды /start
@dp.message_handler(commands=['start'])
@logger.catch
async def welcome(message: types.Message) -> None:
    logging_input_message(message)
    await message.reply('Привет, я бот, который будет тебе отправлять задания и следить за твоими дедлайнами')


@logger.catch
async def send_message_from_user(task_text, user_id, time_for_task):
    try:
        BUTTON_1 = InlineKeyboardButton(
            text='✅ Сделал', callback_data=f"button_{db.get_task_id_by_text(task_text)}|yes")
        BUTTON_2 = InlineKeyboardButton(
            text='❌ Не сделал', callback_data=f"button_{db.get_task_id_by_text(task_text)}|no")
        keyboard = InlineKeyboardMarkup()
        keyboard.row(BUTTON_1, BUTTON_2)
        await bot.send_message(
            user_id, text=f'Ваша задача:\n{task_text}\n\nВремя на ответ: {time_for_task} мин.',
            reply_markup=keyboard)
        return True
    except aiogram.utils.exceptions.ChatNotFound:
        return False


@logger.catch
async def send_message_from_admin(text: str, result, id, username):

    try:
        match result:
            case 'yes':
                out_text = f'Пользователь {username}\n с id: {id}\nуспел выполнить задание:\n\n{text}'
                db.mark_is_done(task_text=text, res=2)
            case 'no':
                out_text = f'Пользователь {username}\n с id: {id}\nне успел выполнить задание:\n\n{text}'
                db.mark_is_done(task_text=text, res=1)
            case 'ignore':
                out_text = f'\nПользователь c id: {id}\nПроигнорировал задание:\n\n{text}'
        await bot.send_message(
            ADMIN_TG_ID, text=out_text
        )
        return True
    except aiogram.utils.exceptions.ChatNotFound:
        return False


@dp.callback_query_handler()
@logger.catch
async def inline_kb_answer(query: types.CallbackQuery):
    data = query.data
    users_text_answer_id = query.from_user.id
    users_text_answer_name = query.from_user.first_name
    if data.split('|')[1] == 'yes':
        await query.answer('Информация отправлена администратору')
        await send_message_from_admin(
            text=db.get_task_by_id(
                int((data.split('|')[0]).replace('button_', ''))
            ),
            result='yes',
            id=users_text_answer_id, username=users_text_answer_name
        )
    elif data.split('|')[1] == 'no':
        await query.answer('Информация отправлена администратору')
        await send_message_from_admin(
            text=db.get_task_by_id(
                int((data.split('|')[0]).replace('button_', ''))
            ),
            result='no',
            id=users_text_answer_id, username=users_text_answer_name
        )


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
