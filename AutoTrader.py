import requests

class Test:
    params = {'page': 2, 'count': 25}
    url="https://httpbin.org"

    def Test(self, params, url):
        self.params = params
        self.url = url

    def connect(self):
        resp = requests.post(self.url, params=self.params)
        print(resp.status_code)
        if resp.ok:
            print(resp.text)



Test().connect()

