import bridge as b

class Translator:

    def __init__(self, sourse : str):
        self.dict = self.download_dict(sourse)

    def download_dict(self, sourse : str):
        d = dict()
        f = open(sourse, 'r')
        for line in f:
            ph = line.split(':')
            d[ph[0]] = ph[1]            
        f.close()
        return d

    def translate(self, word : str):
        if word in self.dict:
            return self.dict[word]
        else:
            return word
    
    def abs(self, number):
        if number < 0:
            return number * -1 
        return number

    def diff_cmp(self, name1, name2, k):
        res1 = 0
        res2 = 0
        name1 = " ".join(name1.split())
        name2 = " ".join(name2.split())
        for c in name1:
            res1 += ord(c)
        for c in name2:
            res2 += ord(c)
        return self.abs(res1 - res2) <= k
