# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
网上搬得一个线程池实现: http://my.oschina.net/zhengyijie/blog/177943
谢谢原作者
'''


import sys
import threading
import queue
import traceback



# 定义一些Exception，用于自定义异常处理
class StlNoResultsPendingException(Exception):
    """All works requests have been processed"""
    pass

class StlNoWorkersAvailableException(Exception):
    """No worket threads available to process remaining requests."""
    pass

def _handle_thread_exception(request, exc_info):
    """默认的异常处理函数，只是简单的打印"""
    traceback.print_exception(*exc_info)


#classes
class StlWorkerThread(threading.Thread):
    """后台线程，真正的工作线程，从请求队列(request_queue)中获取work，
    并将执行后的结果添加到结果队列(result_queue)"""
    def __init__(self,request_queue, result_queue, poll_timeout=5, **kwds):
        threading.Thread.__init__(self, **kwds)
        '''设置为守护进行'''
        self.setDaemon(True)
        self._request_queue = request_queue
        self._result_queue = result_queue
        self._poll_timeout = poll_timeout
        '''设置一个flag信号，用来表示该线程是否还被dismiss,默认为false'''
        self._dismissed = threading.Event()
        self.start()

    def run(self):
        '''每个线程尽可能多的执行work，所以采用loop，
        只要线程可用，并且request_queue有work未完成，则一直loop'''
        while True:
            if self._dismissed.is_set():
                break
            try:
                '''
                queue.Queue队列设置了线程同步策略，并且可以设置timeout。
                一直block，直到request_queue有值，或者超时
                '''
                request = self._request_queue.get(True, self._poll_timeout)
            except queue.Empty:
                continue
            else:
                '''之所以在这里再次判断dimissed，是因为之前的timeout时间里，很有可能，该线程被dismiss掉了'''
                if self._dismissed.is_set():
                    self._request_queue.put(request)
                    break
                try:
                    '''执行callable，讲请求和结果以tuple的方式放入request_queue'''
                    result = request.callable(*request.args, **request.kwds)
                    print(self.getName())
                    self._result_queue.put((request,result))
                except:
                    '''异常处理'''
                    request.exception = True
                    self._result_queue.put((request,sys.exc_info()))

    def dismiss(self):
        '''设置一个标志，表示完成当前work之后，退出'''
        self._dismissed.set()


class StlWorkRequest:
    '''
    @param callable_:，可定制的，执行work的函数
    @param args: 列表参数
    @param kwds: 字典参数
    @param requestID: id
    @param callback: 可定制的，处理result_queue队列元素的函数
    @param exc_callback:可定制的，处理异常的函数
    '''
    def __init__(self, callable_, args=None, kwds=None, requestID=None,
                 callback=None, exc_callback=_handle_thread_exception):
        if requestID == None:
            self.requestID = id(self)
        else:
            try:
                self.requestID = hash(requestID)
            except TypeError:
                raise TypeError("requestId must be hashable")
        self.exception = False
        self.callback = callback
        self.exc_callback = exc_callback
        self.callable = callable_
        self.args = args or []
        self.kwds = kwds or {}

    def __str__(self):
        return "StlWorkRequest id=%s args=%r kwargs=%r exception=%s" % \
            (self.requestID,self.args,self.kwds,self.exception)


class StlThreadPool:
    '''
    @param num_workers:初始化的线程数量
    @param q_size,resq_size: request_queue和result队列的初始大小
    @param poll_timeout: 设置工作线程StlWorkerThread的timeout，也就是等待request_queue的timeout
    '''
    def __init__(self, num_workers, q_size=0, resq_size=0, poll_timeout=5):
        self._request_queue = queue.Queue(q_size)
        self._result_queue = queue.Queue(resq_size)
        self.workers = []
        self.dismissedWorkers = []
        self.work_requests = {} #设置个字典，方便使用
        self.createWorkers(num_workers, poll_timeout)

    def createWorkers(self, num_workers, poll_timeout=5):
        '''创建num_workers个WorkThread,默认timeout为5'''
        for i in range(num_workers):
            self.workers.append(StlWorkerThread(self._request_queue, self._result_queue, poll_timeout=poll_timeout))

    def dismissWorkers(self, num_workers, do_join=False):
        '''停用num_workers数量的线程，并加入dismiss_list'''
        dismiss_list = []
        for i in range(min(num_workers,len(self.workers))):
            worker = self.workers.pop()
            worker.dismiss()
            dismiss_list.append(worker)
        if do_join :
            for worker in dismiss_list:
                worker.join()
        else:
            self.dismissedWorkers.extend(dismiss_list)

    def joinAllDismissedWorkers(self):
        '''join 所有停用的thread'''
        #print len(self.dismissedWorkers)
        for worker in self.dismissedWorkers:
            worker.join()
        self.dismissedWorkers = []

    def putRequest(self,request ,block=True,timeout=None):
        assert isinstance(request, StlWorkRequest)
        assert not getattr(request, 'exception', None)
        '''当queue满了，也就是容量达到了前面设定的q_size,它将一直阻塞，直到有空余位置，或是timeout'''
        self._request_queue.put(request, block, timeout)
        self.work_requests[request.requestID] = request

    def poll(self,block = False):
        while True:
            if not self.work_requests:
                raise StlNoResultsPendingException
            elif block and not self.workers:
                raise StlNoWorkersAvailableException
            try:
                '''默认只要resultQueue有值，则取出，否则一直block'''
                request , result = self._result_queue.get(block=block)
                if request.exception and request.exc_callback:
                    request.exc_callback(request,result)
                if request.callback and not (request.exception and request.exc_callback):
                    request.callback(request,result)
                del self.work_requests[request.requestID]
            except queue.Empty:
                break

    def wait(self):
        while True:
            try:
                self.poll(True)
            except StlNoResultsPendingException:
                break

    def workersize(self):
        return len(self.workers)

    def stop(self):
        '''join 所有的thread,确保所有的线程都执行完毕'''
        self.dismissWorkers(self.workersize(),True)
        self.joinAllDismissedWorkers()


if __name__ == '__main__':
    import random
    import time
    import datetime
    def do_work(data):
        time.sleep(random.randint(1,3))
        res = str(datetime.datetime.now()) + "" +str(data)
        return res

    def print_result(request,result):
        print("---Result from request %s : %r" % (request.requestID, result))

    main = StlThreadPool(3)
    for i in range(40):
        req = StlWorkRequest(do_work, args=[i], kwds={}, callback=print_result)
        main.putRequest(req)
        print("work request #%s added." % req.requestID)

    print('-'*20, main.workersize(),'-'*20)

    counter = 0
    while True:
        try:
            time.sleep(0.5)
            main.poll()
            if(counter == 5):
                print("Add 3 more workers threads")
                main.createWorkers(3)
                print('-'*20, main.workersize(),'-'*20)
            if(counter == 10):
                print("dismiss 2 workers threads")
                main.dismissWorkers(2)
                print('-'*20, main.workersize(),'-'*20)
            counter+=1
        except StlNoResultsPendingException:
            print("no pending results")
            break

    main.stop()
    print("Stop")