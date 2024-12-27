import asyncio
import logging
from tgbot.loader import bot, dp
from tgbot.handlers.user import user_router


logger = logging.getLogger(__name__)


async def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] #%(levelname)-8s %(filename)s:'
               '%(lineno)d - %(name)s - %(message)s'
    )
    logger.info('Starting bot')
    dp.include_router(user_router)
    # Здесь будем регистрировать миддлвари
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.error("Bot stopped!")
