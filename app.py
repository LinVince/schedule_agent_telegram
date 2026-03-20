# @title #Line
from flask import Flask, request, abort, jsonify
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)
import threading
from schedule import start_scheduler, handle_user_text
import logging
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError


# ---- Replace with your real token ----
TELEGRAM_BOT_TOKEN = "8707600308:AAHfWwQRX5bhLRGOHAv_evr7ZxxFuvbUOiY"
# --------------------------------------

app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    force=True,
)

start_scheduler()


@app.route("/", methods=["GET"])
def home():
    instance = os.environ.get("WEBSITE_INSTANCE_ID", "local")
    app.logger.info("AWAKE ping received - instance")
    return jsonify({
        "status": "awake",
        "instance": instance
    }), 200

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hello! I'm your chatbot. How can I help you?")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text
    
    # 👇 use your function here
    response = handle_user_text(user_text)
    
    await update.message.reply_text(response)

async def handle_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Update {update} caused error: {context.error}")

def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(handle_error)

    print("Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    run_flask()
