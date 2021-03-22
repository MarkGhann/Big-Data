class Translator:

    def __init__(self, sourse : str):
        self.dict = self.downloaw_dict(sourse)

    def downloaw_dict(self, sourse : str):
        d = dict()
        f = open(sourse, 'r')
        for line in f:
            ph = line.split(':')
            d[ph[0]] = ph[1]            
        f.close()
        return d

    def translate(self, word : str):
        return self.dict[word]
