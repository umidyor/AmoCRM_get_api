import json
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import BotCommand, BotCommandScopeDefault, BotCommandScopeChat
from bot_env import API_TOKEN,ADMINS


# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


# Define States
class TokenForm(StatesGroup):
    client_id = State()
    client_secret = State()
    code = State()
    redirect_uri = State()

async def problems(message: str):

    for admin_id in ADMINS:
        try:
            await bot.send_message(chat_id=admin_id, text=message)
        except Exception as e:
            print(f"Failed to send message to admin {admin_id}: {e}")
# Helper function to set commands
async def set_commands():
    # Public commands for everyone
    public_commands = [
        BotCommand(command="/start", description="Run this bot"),
    ]
    await bot.set_my_commands(public_commands, scope=BotCommandScopeDefault())

    # Admin commands
    admin_commands = [
        BotCommand(command="/start", description="Run this bot"),
        BotCommand(command="/tokens", description="Get new tokens"),
        BotCommand(command="/stop", description="Stop the form"),
    ]
    for admin_id in ADMINS:
        await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(admin_id))


# Start Command Handler (accessible to everyone)
@dp.message_handler(commands="start")
async def start(message: types.Message):
    await message.reply(f"Salom {message.from_user.full_name} botga xush kelibsiz🚀", parse_mode="Markdown")
    await TokenForm.client_id.set()


# Stop Command Handler (accessible to admins only)
@dp.message_handler(commands="stop", state="*")
async def stop_form(message: types.Message, state: FSMContext):
    if message.from_user.id in ADMINS:
        await state.finish()  # Finish the current form state
        await message.reply("The form has been stopped and the state has been reset.", parse_mode="Markdown")
    else:
        await message.reply("Access denied. This command is for admins only.")


# Collect Tokens Command Handler (accessible to admins only)
@dp.message_handler(commands="tokens")
async def collect_tokens(message: types.Message):
    if message.from_user.id in ADMINS:
        await message.reply("Welcome! Let's collect the data.\n\nPlease enter the *Client ID*:", parse_mode="Markdown")
        await TokenForm.client_id.set()
    else:
        await message.reply("Access denied. This command is for admins only.")


# Client ID Handler
@dp.message_handler(state=TokenForm.client_id)
async def process_client_id(message: types.Message, state: FSMContext):
    # Save client_id
    await state.update_data(client_id=message.text)
    await message.reply("Got it! Now, please enter the *Client Secret*: ")
    await TokenForm.client_secret.set()


# Client Secret Handler
@dp.message_handler(state=TokenForm.client_secret)
async def process_client_secret(message: types.Message, state: FSMContext):
    # Save client_secret
    await state.update_data(client_secret=message.text)
    await message.reply("Thank you! Now, enter the *Code*: ")
    await TokenForm.code.set()


# Code Handler
@dp.message_handler(state=TokenForm.code)
async def process_code(message: types.Message, state: FSMContext):
    await state.update_data(code=message.text)
    await message.reply("Almost done! Please enter the *Redirect URI*: ")
    await TokenForm.redirect_uri.set()


@dp.message_handler(state=TokenForm.redirect_uri)
async def process_redirect_uri(message: types.Message, state: FSMContext):
    # Save redirect_uri
    await state.update_data(redirect_uri=message.text)

    # Retrieve all data from the state
    data = await state.get_data()

    # Save to JSON file
    with open("tokens_file.json", "w") as file:
        json.dump(data, file, indent=4)

    await message.reply("All data has been saved successfully to `tokens_file.json`. Thank you!", parse_mode="Markdown")

    # Finish the conversation
    await state.finish()


# Unknown Input Handler for Ongoing Conversations
@dp.message_handler(state="*")
async def unknown_state_handler(message: types.Message):
    await message.reply("Please follow the instructions or restart by typing /tokens.")


# Start the bot
if __name__ == "__main__":
    from aiogram import executor

    async def on_startup(dispatcher):
        await set_commands()


    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
