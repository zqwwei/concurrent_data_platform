import threading
import requests
import time

def initialize_database(url, db_type, db_url, use_rabbitmq=True):
    data = {
        "db_type": db_type,
        "db_url": db_url,
        "use_rabbitmq": use_rabbitmq
    }
    response = requests.post(url + "init", json=data)
    print(f"状态码: {response.status_code}")
    print(f"响应内容: {response.text}")  # 打印完整的响应内容
    try:
        response_json = response.json()
        print(f"初始化响应: {response_json}")
        return response.status_code == 200
    except ValueError as e:
        print(f"解析JSON响应失败: {e}")
        return False


def send_query_request(url):
    try:
        response = requests.get(url)
        print(f"状态码: {response.status_code} url: {url} 查询响应: {response.json()} ")

    except ValueError as e:
        print(f"解析JSON响应失败: {e}")
        print(f"原始响应内容: {response.text}")
    except Exception as e:
        print(f"请求失败: {e}")

def send_modify_request(url, data):
    try:
        response = requests.post(url, json=data)
        print(f"状态码: {response.status_code}")
        # print(f"原始响应内容: {response.text}")
        # response_json = response.json()
        # print(f"修改响应: {response_json}")
    except ValueError as e:
        print(f"解析JSON响应失败: {e}")
        print(f"原始响应内容: {response.text}")
        # pass
    except Exception as e:
        print(f"请求失败: {e}")
        # pass

# 示例使用
if __name__ == "__main__":
    base_url = "http://localhost:5000/"  # 替换为你的实际服务器地址
    db_type = "mysql"
    db_url = "mysql://root:root@localhost:3306/simpledb"  # 替换为你的实际数据库地址

    # 初始化数据库
    if not initialize_database(base_url, db_type, db_url):
        print("数据库初始化失败")
        exit(1)

    query_cases = [
        "?query=C3 != \"Value 1\" or C1 $= \"test data\"",
        "?query=C1 == \"Sample Text 1\" and C2 &= \"Another\"",
        "?query=C3 != \"Value 1\" or C1 $= \"test data\""
    ]

    modify_cases = [
        # {"job": "INSERT \"Sample Text 3\", \"Another Sample C\", \"Value 3\""},
         {"job": "DELETE \"Sample 3\""},
         
    ]

    # 并发查询测试
    # query_threads = []
    # for query in query_cases:
    #     t = threading.Thread(target=send_query_request, args=(base_url + query,))
    #     t.start()
    #     query_threads.append(t)

    # for t in query_threads:
    #     t.join()

    # print("并发查询完成")

    # 顺序修改测试
    for modify in modify_cases:
        send_modify_request(base_url, modify)

    print("顺序修改完成")
