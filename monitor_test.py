import psycopg2
import psycopg2.extensions
import select
import json
from datetime import datetime
import os
import logging
import time

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
    """设置数据库触发器和通知（监控 messages 表的新行插入和 answer_tokens 更新）"""
    cursor = conn.cursor()
    try:
        # 创建插入触发器（可选，用于跟踪新消息插入）
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

        # 创建更新触发器（监控 answer_tokens 从 0 变为非 0）
        cursor.execute("""
        CREATE OR REPLACE FUNCTION notify_message_update()
        RETURNS TRIGGER AS $$
        BEGIN
            -- 仅当 answer_tokens 从 0 变为非 0 时发送通知
            IF OLD.answer_tokens = 0 AND NEW.answer_tokens != 0 THEN
                BEGIN
                    PERFORM pg_notify('message_updated', row_to_json(NEW)::text);
                EXCEPTION WHEN OTHERS THEN
                    -- 忽略通知错误，不影响事务
                END;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS message_update_trigger ON messages;
        CREATE TRIGGER message_update_trigger
        AFTER UPDATE ON messages
        FOR EACH ROW
        WHEN (OLD.answer_tokens = 0 AND NEW.answer_tokens != 0)
        EXECUTE FUNCTION notify_message_update();
        """)

        conn.commit()
        logging.info("触发器设置成功（监控 messages 插入和 answer_tokens 更新）")
    except Exception as e:
        logging.error(f"设置触发器失败：{e}")
        conn.rollback()
    finally:
        cursor.close()

def process_message(message_data):
    """处理消息数据并格式化输出"""
    try:
        created_at = datetime.fromisoformat(message_data.get('created_at', ''))
        query = message_data.get('query', '未知问题')
        answer = message_data.get('answer', '未知回答')
        from_account_id = message_data.get('from_account_id', '未知用户')
        
        # 清理回答中的冗余标签（根据实际格式调整）
        clean_answer = answer.replace('<details style="color:gray;background-color: #f8f8f8;padding: 8px;border-radius: 4px;" open> <summary> Thinking... </summary>', '')\
                            .replace('</details>', '').strip()
        
        output = [
            f"用户ID：{from_account_id}",
            f"时间：{created_at}",
            f"问题：{query}",
            "回答：",
            clean_answer,
            "="*80,
            ""
        ]
        return '\n'.join(output)
    except Exception as e:
        logging.error(f"处理消息时发生错误：{e}")
        return None
    

def listen_to_database():
    """监听数据库更新并导出记录"""
    while True:
        conn = None
        curs = None
        try:
            # 每次循环重新建立连接
            conn = psycopg2.connect(**DB_CONFIG)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            setup_database_listener(conn)
            
            curs = conn.cursor()
            curs.execute("LISTEN message_updated;")
            logging.info("开始监听消息更新...")

            while True:
                if select.select([conn], [], [], 5) == ([], [], []):
                    continue
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    logging.info(f"收到通知：{notify.channel}, 数据：{notify.payload}")
                    
                    if notify.channel == 'message_updated':
                        message_data = json.loads(notify.payload)
                        formatted = process_message(message_data)
                        if formatted:
                            with open(OUTPUT_FILE, 'a', encoding=ENCODING) as f:
                                f.write(formatted)
                            logging.info(f"已保存更新的消息记录：{message_data['id']}")
        except KeyboardInterrupt:
            logging.info("监听已停止")
            break
        except Exception as e:
            logging.error(f"发生错误：{e}")
            if curs:
                curs.close()
            if conn:
                conn.close()
            time.sleep(5)  # 等待后重试
            continue
        finally:
            if curs:
                curs.close()
            if conn:
                conn.close()

if __name__ == "__main__":
    with open(OUTPUT_FILE, 'w', encoding=ENCODING) as f:
        f.write("聊天记录监控输出\n")
        f.write("="*80 + "\n\n")
    listen_to_database()