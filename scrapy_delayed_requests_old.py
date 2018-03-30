from weakref import WeakKeyDictionary

from scrapy import signals
from scrapy.exceptions import DontCloseSpider, IgnoreRequest
from twisted.internet import reactor


class DelayedRequestsMiddleware(object):
    requests = WeakKeyDictionary()

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        return ext

    @classmethod
    def spider_idle(cls, spider):
        if cls.requests.get(spider):
            spider.log("delayed requests pending, not closing spider")
            raise DontCloseSpider()

    def process_request(self, request, spider):
        delay = request.meta.pop('delay_request', None)
        if delay:
            self.requests.setdefault(spider, 0)
            self.requests[spider] += 1
            reactor.callLater(delay, self.schedule_request, request.copy(),
                              spider)
            raise IgnoreRequest()

    def schedule_request(self, request, spider):
        spider.crawler.engine.schedule(request, spider)
        self.requests[spider] -= 1
