import psycopg2
import psycopg2.extensions
import select
import json
from datetime import datetime
import os
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "dify",
    "user": "postgres",
    "password": "difyai123456"
}

# 输出文件配置
OUTPUT_FILE = r'D:\AI\2025\txts\test_output.txt'
ENCODING = "utf-8"
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

def setup_database_listener(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
        CREATE OR REPLACE FUNCTION notify_new_message()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('new_message', row_to_json(NEW)::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS message_insert_trigger ON messages;
        CREATE TRIGGER message_insert_trigger
        AFTER INSERT ON messages
        FOR EACH ROW EXECUTE FUNCTION notify_new_message();
        """)
        cursor.execute("""
        CREATE OR REPLACE FUNCTION notify_new_conversation()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('new_conversation', row_to_json(NEW)::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS conversation_insert_trigger ON conversations;
        CREATE TRIGGER conversation_insert_trigger
        AFTER INSERT ON conversations
        FOR EACH ROW EXECUTE FUNCTION notify_new_conversation();
        """)
        conn.commit()
        logging.info("触发器设置成功")
    except Exception as e:
        logging.error(f"设置触发器失败：{e}")
        conn.rollback()

def process_message(message_data):
    try:
        created_at = datetime.fromisoformat(message_data.get('created_at', ''))
        query = message_data.get('query', '未知问题')
        answer = message_data.get('answer', '未知回答')
        clean_answer = answer.replace('<details style="color:gray;background-color: #f8f8f8;padding: 8px;border-radius: 4px;" open> <summary> Thinking... </summary>', '')\
                            .replace('</details>', '').strip()
        output = [f"时间：{created_at}", f"问题：{query}", "回答：", clean_answer, "="*80, ""]
        return '\n'.join(output)
    except Exception as e:
        logging.error(f"处理消息时发生错误：{e}")
        return None

def listen_to_database():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    setup_database_listener(conn)
    
    curs = conn.cursor()
    curs.execute("LISTEN new_message;")
    curs.execute("LISTEN new_conversation;")
    logging.info("开始监听数据库变更...")

    while True:
        try:
            if select.select([conn], [], [], 5) == ([], [], []):
                continue
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                logging.info(f"收到通知：{notify.channel}, 数据：{notify.payload}")
                
                if notify.channel == 'new_message':
                    message_data = json.loads(notify.payload)
                    formatted = process_message(message_data)
                    if formatted:
                        with open(OUTPUT_FILE, 'a', encoding=ENCODING) as f:
                            f.write(formatted)
                        logging.info(f"已保存新消息记录：{message_data['id']}")
                elif notify.channel == 'new_conversation':
                    conversation_data = json.loads(notify.payload)
                    logging.info(f"检测到新会话：{conversation_data['id']}")
        except KeyboardInterrupt:
            logging.info("监听已停止")
            break
        except Exception as e:
            logging.error(f"发生错误：{e}")
            time.sleep(5)
            conn = psycopg2.connect(**DB_CONFIG)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    curs.close()
    conn.close()

if __name__ == "__main__":
    with open(OUTPUT_FILE, 'w', encoding=ENCODING) as f:
        f.write("聊天记录监控输出\n")
        f.write("="*80 + "\n\n")
    listen_to_database()