import asyncio
import base64 as Base64
import math
import os
import re
from functools import reduce
from hashlib import md5 as MD5
from multiprocessing import Manager, Pool

import requests
import time
from Crypto.Cipher import AES
from aiohttp import ClientSession, ClientTimeout


def url_to_str(url):
    """
    通过 url 获得一个字符串

    :param url:
    :return:
    """
    return Base64.b16encode(MD5(url.encode()).digest()).decode('utf-8')


def is_complete_url(url):
    """
    验证 url 是否完整

    :param url:
    :return:
    """
    re_result = re.match("http.*", url)

    if re_result is None:
        return False

    return True


def zero_fill(value, length=5):
    """
    用0 填充

    :param value:
    :param length:
    :return:
    """
    format_str = "%0" + str(length) + "d_"
    return format_str % value


def get_file_list(folder_path, re_compile=None, re_method="match", abs_path=True):
    """
    获取文件列表 通过文件夹路径

    :param folder_path:
    :param re_compile:
    :param re_method:
    :return:
    """

    file_list = []
    re_method_dict = {}

    if re_compile is not None:
        re_method_dict = {
            "match": re_compile.match
            , "search": re_compile.search
        }

    for dirpath, dirnames, filenames in os.walk(folder_path):
        if re_compile is None:
            file_list = filenames
            break
        for element in filenames:
            result = re_method_dict[re_method](element)  # type: re

            if result.group() is None:
                continue

            if not abs_path:
                file_list.append(element)
                continue

            abs_path = os.path.abspath(dirpath) + "/" + element
            file_list.append(abs_path)

    return file_list


def get_loop_num_and_size(dispose_list, max_size=50, power_num=3):
    """
    对列表分批处理
    获取循环次数 / 分几批处理
    以及 一次处理的数量

    :param dispose_list: 待处理的列表
    :param max_size:  一次处理的数量(最大 50)
    :param power_num: 次方(默认 3) , 通过Cpu 核心数计算一次处理的数量 ,  (Cpu 核心数) ^ (power_num)
    :return:
    """

    # 每次处理的数量
    dispose_size = max_size if os.cpu_count() is None or math.pow(os.cpu_count(), power_num) > max_size \
        else math.pow(os.cpu_count(), power_num)

    # 计算分批数 counter
    quotient, remainder = len(dispose_list) // dispose_size, len(dispose_list) % dispose_size
    counter = quotient + 1 if remainder > 0 else quotient

    return counter, dispose_size


class Video(object):

    def __init__(self, key, url, method):
        """
        视频切片

        :param key:
        :param url:
        """
        self.key = key
        self.method = method
        self.url = url

        self.file_name = "" if url is None else url_to_str(url)


class M3U8(object):

    def __init__(self, url, file_context, root_url=None):
        """
        初始化

        :param url: url
        :param file_name: 文件名
        :param file_context: 文件内容
        """
        # m3u8 文件的 url 来源 url
        self.root_url = root_url

        # m3u8 文件的 url
        self.url = url

        # 如果 self.url 并不包含视频文件
        # 将包含另一个 m3u8_url
        self.m3u8_url = ""

        # 视频文件 url 前半部分
        self.video_url_prefix = url[:url.rfind("/")]

        # 视频文件 url 前半部分 , 通过 / 分割
        self.video_url_prefix_list = self.video_url_prefix.split("/")

        # 通过 url 得到文件名
        self.file_name = url_to_str(self.url)

        # m3u8 url 文件内容
        self.file_context = file_context

        # 视频文件的总个数
        self.video_num = 0

        # 当前视频文件的索引
        self.video_num_index = 0

        # 视频文件的总大小
        self.video_size = 0

        # 视频文件 已合并的大小
        self.video_size_sum = 0

        self.video_list = []

        self.get_m3u8_url_by_context()
        self.get_video_by_context()

    def get_m3u8_url_by_context(self):
        """
        根据文件内容获取 m3u8 url

        :return:
        """

        # 正则, 确定文件内容是否含有 m3u8
        # 如果有, 则继续判断 m3u8 url 是否是完整路径(通过是否含有http 确定)
        # 如果没有, 则拼接完整 url 前缀

        result = re.search(".*?m3u8", self.file_context, re.IGNORECASE)

        if result is not None:
            self.m3u8_url = result.group()
            m3u8_url_list = self.m3u8_url.split("/")

            if not is_complete_url(self.m3u8_url):

                # 如果 m3u8_url 和 url 存在重复,
                # 则去除重复部分
                # url https://v8.dious.cc/20210428/xyp1ZZX6/index.m3u8
                # m3u8_url /20210428/xyp1ZZX6/1000kb/hls/index.m3u8
                # 去重后,最终 m3u8_url https://v8.dious.cc/20210428/xyp1ZZX6/1000kb/hls/index.m3u8

                for item in m3u8_url_list:
                    if item not in self.video_url_prefix_list:
                        self.video_url_prefix_list.append(item)

                self.m3u8_url = "/".join(self.video_url_prefix_list)

    def get_video_by_context(self):
        """
        根据文件内容获取视频切片 url

        :return:
        """

        if "#EXTINF".lower() not in self.file_context.lower():
            return []
        # 假设所有的 key 都在视频切片前面

        # 按行存储
        context_line_list = self.file_context.split("\n")

        # 遍历行 查找 key , 查找视频切片 url

        key = ""  # 秘钥
        method = ""  # 加密方法
        video_line = 0  # 视频行 行号

        for index, context_line in enumerate(context_line_list):

            # 判断 key
            if "#EXT-X-KEY:".lower() in context_line.lower():

                # 1 EXT-X-KEY:METHOD=NONE
                # 2 EXT-X-KEY:METHOD=AES-128,URI="https://ts8.510yh.cc:8899/20210428/xyp1ZZX6/1000kb/hls/key.key"
                # 2 是目标值

                if "," not in context_line:
                    # 如果是 1  method = None
                    method = None
                    key = None
                    continue

                # 如果是 2 ,  则进行处理
                # 弃用 通过 , 分割成两部分 . 再通过 = 进行二次分割获取 key_url 和 method
                # 通过 正则 获取 key_url 和 method

                re_result = re.match(".*?=(.*?),.*?=\"(.*?)\"", context_line)
                method = re_result.group(1)
                key_url = re_result.group(2)

                # key_url 是否完整
                if "http" not in key_url:
                    # 不完整 要做啥, 写在这 , 现在先不写
                    pass

                # key_url 完整, 通过 key_url 获取 key
                key = requests.get(key_url).content.decode(encoding="utf8", errors="ignore")

            # 判断 视频切片标志
            if "#EXTINF:".lower() in context_line.lower():
                # 如果存在标志, 则下一行即为视频 url
                video_line = index + 1
                continue

            # 处理视频 url
            if video_line == index and video_line > 0:

                video_url = context_line

                # 废弃 如果包含 http 则认为是一个完整 url
                # 使用正则处理

                if not is_complete_url(video_url):
                    # video = self.url_prefix + "/" + video
                    video_url = self.video_url_prefix + video_url

                obj_video = Video(key, video_url, method)
                self.video_list.append(obj_video)


class DownLoader(object):

    def __init__(self, url_list):
        self.list_m3u8_file_url = url_list
        self.list_m3u8_obj = []

        # 消息队列
        self.pool_queue = Manager().Queue()

        # 进度日志
        self.progress_log = []

        # 消息队列
        self.pool_queue_out = None

    def get_m3u8_context_by_url(self):
        """
        #1. 获取m3u8 文件
        #   判断文件内容是视频 url 还是 m3u8 url
        #       是 m3u8 url        执行 1
        #       不是 m3u8 url      执行 2

        :return:
        """

        for element in self.list_m3u8_file_url:

            root_url = ""
            m3u8_file_url = element

            if isinstance(m3u8_file_url, tuple):
                root_url, m3u8_file_url = element

            # url 完整性校验
            if m3u8_file_url is None \
                    or len(m3u8_file_url) == 0 \
                    or not is_complete_url(m3u8_file_url):
                continue

            # 请求起始 m3u8_file_url
            file_context = requests.get(m3u8_file_url).content

            # 为空返回
            if file_context is None:
                continue

            # 获取内容
            file_context = file_context.decode(encoding="utf8", errors="ignore")

            # 构建 m3u8_obj , 通过 m3u8_obj 获取url
            m3u8_obj = M3U8(m3u8_file_url, file_context, root_url=root_url)

            # 通过#EXTINF 判断是不是切片文件
            # 不是切片文件, 加入 list_m3u8_file_url 列表
            if "#EXTINF".lower() not in file_context.lower():
                self.list_m3u8_file_url.append((m3u8_file_url, m3u8_obj.m3u8_url))
                continue

            self.list_m3u8_obj.append(m3u8_obj)

    def downloading(self, queue=None):
        """
        开始下载

        :return:
        """
        if queue is not None:
            self.pool_queue_out = queue

        # 通过URL 获得 M3U8 文件内容
        self.get_m3u8_context_by_url()

        if len(self.list_m3u8_obj) == 0:
            return

        pool = Pool(processes=os.cpu_count() + 1)
        pool.apply_async(self.test_queue)

        for index, m3u8_obj in enumerate(self.list_m3u8_obj):  # type: M3U8

            # 获取视频切片 & 合并文件
            pool.apply_async(self.task_download_video_and_merge_file, args=(m3u8_obj, index))

        pool.close()
        pool.join()

    def task_download_video_and_merge_file(self, m3u8_obj, obj_index):
        """
        获取视频切片 & 合并文件

        :return:
        """

        file_name = m3u8_obj.file_name

        # 创建文件夹
        folder_name_path = "./" + zero_fill(obj_index) + str(file_name)
        if not os.path.isdir(folder_name_path):
            os.mkdir(folder_name_path)

        # 下载视频切片
        # files_size_list = [{"video_num": video_num , "video_size": video_size , "video_name": video_name }
        #                   , {"video_num": video_num , "video_size": video_size , "video_name": video_name } ]
        files_size_list = self.pretreatment_download_video_fragment(folder_name_path, m3u8_obj)

        Video_size_list = []
        # 计算视频文件的总大小
        for file_info in files_size_list[:]:
            if file_info is None:
                files_size_list.remove(file_info)
                continue

            m3u8_obj.video_size += file_info["video_size"]
            Video_size_list.append(file_info["video_size"])

        # 合并视频切片
        self.pretreatment_merge_video(folder_name_path, Video_size_list, m3u8_obj.root_url)

    def pretreatment_download_video_fragment(self, file_name_path, m3u8_obj):
        """
        下载视频片段预处理

        :param file_name_path:  文件夹路径
        :param obj:
        :return:
        """

        # 分批处理
        # 循环次数 counter  , 每次处理的大小 dispose_size
        counter, dispose_size = get_loop_num_and_size(m3u8_obj.video_list)

        result = []

        for counter_index in range(counter):

            # if counter_index > 0:
            #     # TODO 测试
            #     return result

            tasks = []
            loop = asyncio.get_event_loop()

            # 本次批处理 起始位置
            init_index = counter_index * dispose_size

            # 创建视频切片文件
            for video_index, video in enumerate(
                    m3u8_obj.video_list[init_index:(counter_index + 1) * dispose_size]):
                # 文件编号
                video_num = init_index + video_index

                video_path = file_name_path + "/" \
                             + zero_fill(video_num) \
                             + video.file_name + ".MP4"  # type: Video

                # 下载视频切片
                task = asyncio.ensure_future(
                    self.download_video_fragment(m3u8_obj.root_url, video_num, video_path, video))
                tasks.append(task)

            task_result = loop.run_until_complete(asyncio.gather(*tasks))

            result.extend(task_result)

        return result

    async def download_video_fragment(self, root_url, video_num, video_path, video, num=0):
        """

        下载视频片段

        :param video_num: 视频顺序号
        :param video_path: 切片地址
        :param video: 切片对象
        :param num: 调用次数
        :return:
        """

        key = video.key
        num += 1

        cryptor = None if key is None else AES.new(key.encode('utf-8'), AES.MODE_CBC, key.encode('utf-8'))

        with open(video_path, "wb+") as wf:

            # num<=2 使用异步 io
            # > 2 使用 requests
            try:
                if num > 2:
                    file_context = await requests.get(video.url).content
                    file_context = file_context if cryptor is None else (cryptor.decrypt(file_context))
                    wf.write(file_context)

                    self.pool_queue.put({"root_url": root_url, "status": "download_num"})

                    return {"video_num": video_num
                        , "video_size": len(file_context)
                        , "video_name": video.file_name
                            }
            except Exception as e:
                if num > 4:
                    raise e

                await self.download_video_fragment(root_url, video_num, video_path, video, num)

            try:
                async with ClientSession(timeout=ClientTimeout(total=0.5 * 60)) as session:

                    async with session.get(video.url) as response:
                        file_context = await response.read()
                        file_context = file_context if cryptor is None else (cryptor.decrypt(file_context))
                        wf.write(file_context)

                        self.pool_queue.put({"root_url": root_url, "status": "download_num"})

                        return {"video_num": video_num
                            , "video_size": len(file_context)
                            , "video_name": video.file_name
                                }

            except Exception as e:
                if num > 4:
                    raise e

                await self.download_video_fragment(root_url, video_num, video_path, video, num)

    def pretreatment_merge_video(self, folder_name_path, files_size_list, root_url):
        """
        合并视频预处理

        :param folder_name_path:  待处理的文件夹路径(完整路径)
        :param files_size_list:
        :return:
        """

        # 根据 folder_name_path 获取文件夹下的所有视频片段
        #
        re_compile = re.compile(".*\.mp4", re.I)
        file_abspath_list = get_file_list(folder_name_path, re_compile=re_compile)

        # 根据 folder_name_path 创建一个同名的 MP4 文件
        merge_file_path = folder_name_path + ".mp4"

        range_num = len(file_abspath_list)

        # 分批处理
        # 循环次数 counter  , 每次处理的大小 dispose_size
        counter, dispose_size = get_loop_num_and_size(file_abspath_list)

        with open(merge_file_path, "wb+") as wf:
            for counter_index in range(counter):
                tasks = []
                loop = asyncio.get_event_loop()

                # 本次批处理 起始位置
                init_index = counter_index * dispose_size

                # 创建视频切片文件
                for video_index, video in enumerate(
                        file_abspath_list[init_index:(counter_index + 1) * dispose_size]):
                    # 目标位置 = 起始位置 + 本次位置
                    target_index = init_index + video_index

                    write_location = reduce(lambda x, y: x + y, files_size_list[:target_index + 1],
                                            - files_size_list[0])

                    file_path = file_abspath_list[target_index]

                    # 合并视频切片
                    task = asyncio.ensure_future(self.merge_file(file_path, wf, write_location, root_url))
                    tasks.append(task)

                loop.run_until_complete(asyncio.gather(*tasks))

    async def merge_file(self, file_path, wf, seek, root_url):
        wf.seek(seek)

        with open(file_path, "rb") as rb:
            context = rb.read()
            wf.write(context)
        print(" merge_file ")
        self.pool_queue.put({"root_url": root_url, "status": "merge_num"})

    def test_queue(self, queue=None):

        # 通过 self.list_m3u8_obj 获得每个 m3u8 对象的 视频数量
        # 通过视频数量 构建字典 视频数量 下载数 合并数 video_num download_num merge_num
        # 通过 下载数 合并数
        #                   判断当前处于 下载中 还是 合并文件中
        #                   判断是否结束 进程对队列的读取操作

        # 构建 字典
        for m3u8_obj in self.list_m3u8_obj:
            self.progress_log.append({
                "root_url": m3u8_obj.root_url
                , "video_num": len(m3u8_obj.video_list)
                , "download_num": len(m3u8_obj.video_list)
                , "merge_num": len(m3u8_obj.video_list)
            })

        while True:

            # 处理: 通过获得所有 m3u8 对象的下载数 以及 合并数的 总和 确定是否停止处理
            download_merge_list = re.findall("download_num.*?(\d+)|.merge_num.*?(\d+)", str(self.progress_log))
            stop_flag_list = []
            stop_flag_list.extend(y if x == '' else x for x, y in download_merge_list)
            stop_flag = reduce(lambda x, y: int(x) + int(y), stop_flag_list)

            if stop_flag == 0:
                return

            # 处理: 读取 下载数 合并数
            #                           获得当前的状态
            #                           修改下载数 合并数

            result = self.pool_queue.get()

            for log_obj in self.progress_log:

                if result["root_url"] == log_obj["root_url"]:
                    log_obj[result["status"]] -= 1

                if self.pool_queue_out is not None:
                    self.pool_queue_out.put(self.progress_log)


if __name__ == '__main__':
    list_url = ["https://v8.dious.cc/20210428/xyp1ZZX6/index.m3u8"
        , "https://v8.dious.cc/20210428/au26ievJ/index.m3u8"]

    loader = DownLoader(list_url)
    loader.downloading()
