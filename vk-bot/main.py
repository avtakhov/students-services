import os
import json
import time
import logging
from datetime import datetime

from dotenv import load_dotenv
import requests
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.utils import get_random_id

# ===================== CONFIG & LOGGING =====================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("vk-bot")

VK_TOKEN = os.getenv("VK_TOKEN", "").strip()
VK_GROUP_ID_RAW = os.getenv("VK_GROUP_ID", "").strip()
SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8000").rstrip("/")


if not VK_TOKEN:
    raise RuntimeError("VK_TOKEN пуст. Укажите групповой токен в .env")

try:
    VK_GROUP_ID = int(VK_GROUP_ID_RAW)
except ValueError:
    raise RuntimeError(f"VK_GROUP_ID должен быть числом, получили: {VK_GROUP_ID_RAW!r}")

API_VERSION = os.getenv("VK_API_VERSION", "5.199")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "5.0"))
LP_RETRY_DELAY = float(os.getenv("LP_RETRY_DELAY", "3.0"))

# ===================== BOT CORE =====================

class VKPromoBot:
    def __init__(self, token: str, group_id: int, server_url: str, api_version: str):
        self.group_id = group_id
        self.server_url = server_url

        self.vk_session = vk_api.VkApi(token=token, api_version=api_version)
        self.vk = self.vk_session.get_api()
        self.longpoll = VkBotLongPoll(self.vk_session, group_id)

        logger.info("LongPoll инициализирован. group_id=%s api_version=%s", group_id, api_version)

    # -------- UTILS --------

    def _post_event(self, payload: dict) -> bool:
        try:
            resp = requests.post(f"{self.server_url}/events", json=payload, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                logger.info("Событие отправлено: %s", payload.get("event_type"))
                return True
            logger.error("Сервер вернул %s: %s", resp.status_code)
        except requests.RequestException as e:
            logger.error("Ошибка HTTP при отправке события: %s", e)
        return False

    def _now_iso(self) -> str:
        return datetime.utcnow().isoformat()

    def _get_user_info(self, user_id: int):
        try:
            info = self.vk.users.get(user_ids=user_id)[0]
            return info
        except Exception as e:
            logger.error("Ошибка users.get: %s", e)
            return None

    def _send_message(self, user_id: int, text: str):
        try:
            self.vk.messages.send(
                user_id=user_id,
                message=text,
                random_id=get_random_id(),
                dont_parse_links=1
            )
        except Exception as e:
            logger.error("Ошибка messages.send: %s", e)

    # -------- EVENT BUILDERS --------

    def _build_event(self, event_type: str, user_info: dict, payload_extra: dict | None = None):
        payload = {
            "event_type": event_type,
            "timestamp": self._now_iso(),
            "vk_user_info": {
                "id": user_info["id"],
                "first_name": user_info.get("first_name", ""),
                "last_name": user_info.get("last_name", ""),
            },
            "payload": {}
        }
        if payload_extra:
            payload["payload"].update(payload_extra)
        return payload

    # -------- HELPERS --------


    @staticmethod
    def _extract_qr_id_from_message(message_obj: dict) -> str | None:

        payload = message_obj.get("payload")
        if payload:
            try:
                data = json.loads(payload) if isinstance(payload, str) else payload
                qr = data.get("qr_id")
                if qr:
                    return str(qr)
            except Exception:
                pass

        ref = message_obj.get("ref") or message_obj.get("ref_source")
        return str(ref) if ref else None

    def _is_first_message(self, user_id: int) -> bool:

        try:
            history = self.vk.messages.getHistory(
                user_id=user_id,
                count=2
            )


            message_count = history.get("count", 0)

            logger.debug("История для user_id=%s: count=%s", user_id, message_count)

            return message_count <= 1

        except Exception as e:
            logger.error("Ошибка при получении истории для user_id=%s: %s", user_id, e)

            return False
    # -------- HANDLERS --------

    def handle_start_button(self, user_id: int):
        """Обработчик нажатия кнопки 'Начать'"""
        user = self._get_user_info(user_id)
        if not user:
            return

        welcome_text = (
            "Привет, студент!👋\n"
            "Мы помогаем с любыми видами академических работ.\n\n"
            "Хочешь получить бесплатную работу?\n"
            "[https://vk.com/app7685942_-220116264|👉Крути рулетку]🎯\n\n"
            "Если нужна помощь прямо сейчас — просто напиши сюда, что тебе нужно — например: курсовая, статья или диплом✍️\n\n"
            "Твой личный менеджер ответит в течение 3 минут.\n\n"
            "Для подписчиков действует скидка 30% на первый заказ!\n"
            "[https://vk.com/studgenius|👉Подписаться]💥"
        )

        self._send_message(user_id, welcome_text)
        logger.info("Отправлено приветствие пользователю %s (кнопка Начать)", user_id)

    def handle_message_new(self, obj: dict):
        # Унифицируем структуру входного объекта
        message = obj.get("message") or obj

        user_id = message.get("from_id")
        if not user_id:
            return

        # Берём текст (в некоторых версиях он лежит на верхнем уровне)

        # Остальная логика обработки сообщений
        user = self._get_user_info(user_id)
        if not user:
            return

        ref = None
        payload = message.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = None

        if isinstance(payload, dict):
            ref = payload.get("ref") or payload.get("qr_id") or payload.get("key")

        if not ref:
            ref = (message.get("ref")
                   or obj.get("ref")
                   or message.get("ref_source")
                   or obj.get("ref_source"))

        event_payload = {"qr_id": ref} if ref else None
        if event_payload:
            event = self._build_event("Link", user, event_payload)
            self._post_event(event)

        text = (message.get("text") or obj.get("text") or "").strip().lower()

        # Проверяем системную кнопку "Начать"
        if text == "начать":
            self.handle_start_button(user_id)
            return  # ⚠️ Обязательно прерываем, чтобы не шло дальше

        # Иногда системная кнопка может прийти с payload
        payload_raw = message.get("payload")
        if payload_raw:
            try:
                payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
                if isinstance(payload, dict) and payload.get("command") == "start":
                    logger.info("Пользователь %s нажал кнопку 'Начать' (через payload)", user_id)
                    self.handle_start_button(user_id)
                    return
            except Exception:
                pass


    def handle_group_join(self, obj: dict):
        user_id = obj.get("user_id")
        if not user_id:
            return

        user = self._get_user_info(user_id)
        if not user:
            return

        join_type = obj.get("join_type")
        event = self._build_event("Subscribe", user, {"join_type": join_type} if join_type else None)
        self._post_event(event)



    def handle_group_leave(self, obj: dict):
        user_id = obj.get("user_id")
        if not user_id:
            return

        user = self._get_user_info(user_id)
        if not user:
            return

        self_leave = obj.get("self")
        extra = {"self_leave": bool(self_leave)} if self_leave is not None else None
        event = self._build_event("Unsubscribe", user, extra)
        self._post_event(event)

    # -------- LONGPOLL LOOP --------

    def run(self):
        logger.info("Старт LongPoll. group_id=%s server_url=%s", self.group_id, self.server_url)

        while True:
            try:
                for event in self.longpoll.listen():
                    try:
                        et = event.type

                        logging.debug("RAW EVENT: %s | object=%s", et, getattr(event, "object", None))

                        if et == VkBotEventType.MESSAGE_NEW:
                            obj = event.object if isinstance(event.object, dict) else {}
                            self.handle_message_new(obj)

                        elif et == VkBotEventType.GROUP_JOIN:
                            obj = event.object if isinstance(event.object, dict) else {}
                            self.handle_group_join(obj)

                        elif et == VkBotEventType.GROUP_LEAVE:
                            obj = event.object if isinstance(event.object, dict) else {}
                            self.handle_group_leave(obj)


                    except Exception as e:
                        logger.error("Ошибка обработки события: %s", e, exc_info=True)

            except KeyboardInterrupt:
                logger.info("Остановка по Ctrl+C")
                break
            except Exception as e:
                logger.error("LongPoll отвалился: %s — переподключаюсь через %.1fs", e, LP_RETRY_DELAY)
                time.sleep(LP_RETRY_DELAY)
                try:
                    self.longpoll = VkBotLongPoll(self.vk_session, self.group_id)
                    logger.info("LongPoll переподключён")
                except Exception as e2:
                    logger.error("Не удалось переподключить LongPoll: %s", e2)
                    time.sleep(LP_RETRY_DELAY)


def main():
    bot = VKPromoBot(VK_TOKEN, VK_GROUP_ID, SERVER_URL, API_VERSION)
    bot.run()


if __name__ == "__main__":
    main()
