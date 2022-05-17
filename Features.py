import pandas as pd


class Calc_feature():
    def __init__(self, dataset, col,lista,name):
        self.dataset = dataset
        self.col = col
        self.lista = lista
        self.name = name

    def calculate_feature(self,seq):
        from propythia.sequence import ReadSequence
        from propythia.descriptors import Descriptor
        sequence = ReadSequence()
        ps = sequence.read_protein_sequence(seq)
        protein = Descriptor(ps)
        feature = protein.adaptable(self.lista, lamda_paac=4, lamda_apaac=4)
        res = {'sequence': seq}
        res.update(feature)
        return res

    def features_all(self):
        from joblib import Parallel, delayed
        from multiprocessing import cpu_count
        r = Parallel(n_jobs=int(0.8 * cpu_count()))(delayed(self.calculate_feature)(seq) for seq in self.dataset[self.col])
        return r

    def run(self):
        Feature = self.features_all()
        list_feature = []
        for i in Feature:
            list_feature.append(i)
        self.feature = list_feature
        return list_feature

    def toDataframe(self):
        df = pd.DataFrame(self.feature)
        df.to_csv(self.name)





