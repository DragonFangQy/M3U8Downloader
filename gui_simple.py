import threading
from queue import Queue
from time import sleep
from tkinter import *
from tkinter import messagebox  # 这个是消息框，对话框的关键
from tkinter import ttk

import multiprocessing

from m3u8_downloader import DownLoader

from multiprocessing import Manager, Process


def download():
    str_text = m3u8_url_text.get("0.0", "end")

    if str_text is None or len(str_text) == 0 or "http" not in str_text:
        messagebox.showinfo("提示", "请输入正确的 m3u8 url")
        return

    list_url = str_text.split("\n")

    loader = DownLoader(str_text.split("\n"))

    # Manager().Queue()
    # queue.Queue()
    # multiprocessing.Queue()

    list_ = Manager().Queue()

    queue = list_

    t1 = threading.Thread(target=loader.downloading, args=(queue,))
    t2 = threading.Thread(target=func, args=(list_url, queue))
    t1.start()
    t2.start()


def func(url_list, queue):
    hint_text = "准备下载 ... \n"

    for url_info in url_list:
        if "http" not in url_info:
            continue
        result_text.insert("0.0", hint_text)

    while True:

        if queue.empty():
            continue

        break

    # 停止标识
    stop_flog_list = {}
    while True:

        process_list = {}

        if queue.empty():
            if str(list(stop_flog_list.values())).count("True") == len(url_list):
                print(" stop_flog ")
                break
            continue

        for log_info in queue.get():

            for url_info in url_list[:]:
                if "http" not in url_info:
                    url_list.remove(url_info)
                    continue

                if url_info == log_info["root_url"]:

                    status = "下载中 %s/%s"
                    progress_num = int(log_info["video_num"]) - int(log_info["download_num"])

                    if log_info["download_num"] == 0:
                        status = "合并中 %s/%s"
                        progress_num = int(log_info["video_num"]) - int(log_info["merge_num"])

                        if int(log_info["merge_num"]) == 0:
                            stop_flog_list[url_info] = True
                            process_list[url_info] = "合并完成"
                            continue
                    text_ = status % (progress_num, log_info["video_num"])
                    process_list[url_info] = text_

        result_text.delete("0.0", 'end')
        result_text.insert("0.0", "\n".join(process_list.values()))

        print(list(stop_flog_list.values()))


if __name__ == '__main__':
    # 初始 m3u8_url 列表
    print("https://v8.dious.cc/20210428/xyp1ZZX6/index.m3u8")
    print("https://v8.dious.cc/20210428/au26ievJ/index.m3u8")

    print("https://v8.dious.cc/20210428/JEo7OY7j/index.m3u8")
    print("https://v8.dious.cc/20210428/PD4rIINw/index.m3u8")

    list_url_process = []

    root = Tk()
    root.title("M3U8DownLoader by DragonFang")
    root.geometry("800x800")

    mainframe = ttk.Frame(root, padding="10")
    mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    m3u8_url_label = ttk.Label(mainframe, text="请输入m3u8 url, 每行一条", padding="0 0 0 10")
    m3u8_url_label.grid(column=0, row=0, sticky=W)

    m3u8_url_text = Text(mainframe, width=80, height=30)  # 原始数据录入框
    m3u8_url_text.grid(column=0, row=1, sticky=W)

    result_text = Text(mainframe, width=20, height=30)  # 进度提示框
    result_text.grid(column=1, row=1, sticky=W)

    download_btn = ttk.Button(mainframe, text="Download", command=download)
    download_btn.grid(column=0, row=2, sticky=W, pady="10")

    root.mainloop()
