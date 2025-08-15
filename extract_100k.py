"""
Script pour extraire les 100 000 premières lignes d'un fichier texte source
et les écrire dans un nouveau fichier cible.
"""

SRC = r"C:\Users\maxam\Desktop\neg_sents_comb.txt"
DST = r"C:\Users\maxam\Desktop\trans_sent\data\raw\negation_medical.txt"
N = 100_000

def extract_first_n_lines(src, dst, n):
    count = 0
    with open(src, 'r', encoding='utf-8') as fin, open(dst, 'w', encoding='utf-8') as fout:
        for i, line in enumerate(fin):
            if i >= n:
                break
            fout.write(line)
            count += 1
    print(f"{count} lignes copiées de {src} vers {dst}")

if __name__ == "__main__":
    extract_first_n_lines(SRC, DST, N)
