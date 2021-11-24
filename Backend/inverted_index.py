from resources.tweets import creator_index
import json
import math
from tw_proccesing import TweetProccesor
import os
import sys
from glob import glob
from heapq import heappop, heappush, heapify
sys.path.append('resources')

size_tweets = 20000
tp = TweetProccesor()


class InvertedIndex:

    # @Input: La cantidad de sub-índices a crear y el tamaño de estos. Son dados por la aplicación.
    # @Return: Esctritura de n sub-ínbdices y las longitudes de cada tweet que se leyó.
    # :: Aplica el algoritmo SPIMI para trabajar el indexado en memoria secundaria
    # :: Se construyen varios índices secundarios pequeños
    def __build_inverted_index(self, n, block):
        tw_index = 1
        lengths = {}
        # Accedemos al contenido de los tweets
        with open("resources/data.json", "r") as file:
            # Partición por bloques
            for j in range(math.ceil(n/block)):
                term_freq = {}
                # Partición por tweets
                for i in range(block):
                    tweet_data = file.readline()
                    tweet_data = json.loads(tweet_data)
                    tokens = tp.tokenize(tweet_data["content"])
                    lengths[tw_index] = len(tokens)
                    terms = {}
                    # Construye la tupla (cant_palabras, tw_index) para cada termino
                    for token in tokens:
                        if terms.get(token) is None or terms.get(token)[1] != tw_index:
                            terms[token] = (1, tw_index)
                        else:
                            terms[token] = (terms[token][0] + 1, tw_index)
                    # Se hacen la ponderación por pesos tf-idf
                    # Podemos amortiguar el TF con el logaritmo, por qué solo se pasa por un tweet una vez
                    # No podemos hacer esto con DF, ya que pueden haber más repeticiones del término en otros tweets.
                    for term in terms:
                        if term_freq.get(term) is None:
                            term_freq[term] = [(1 + math.log10(terms[term][0]), terms[term][1])]
                        else:
                            term_freq[term].append((1 + math.log10(terms[term][0]), terms[term][1]))
                    tw_index += 1

                inverted_index = {}
                for word in term_freq:
                    # Guardamos las frecuencias de cada termino:
                    #  - DF: Cantidad de tweets en los que aparece el término al menos una vez
                    #  - TF: Cantidad, amortizada con el logaritmo, de ocurrencias del término 
                    inverted_index[word] = {
                        "DF": len(term_freq[word]),
                        "TF": term_freq[word]
                    }

                # Escritura del i-ésimo índice temporal local
                json_file = open('resources/indexs/i' + str(j) + '.json', 'a', newline='\n', encoding='utf8')
                json_file.truncate(0)
                json_file.write(json.dumps(inverted_index, ensure_ascii=False, default=str))

        # Escritura de las longitudes de los tweets
        lengths_file = open('resources/lengths.json', 'a', newline='\n', encoding='utf8')
        lengths_file.truncate(0)
        lengths_file.write(json.dumps(lengths, ensure_ascii=False, default=str))
        return

    # @Input: Los n bloques que se particionaron en build_inverted_index
    # @Return: El índice invertido completo
    def __merge(self):
        inverted_index = {}

        # Lectura de los índices intermedios
        for f_name in glob('resources/indexs/*.json'):
            with open(f_name, "r") as index:
                i_dic = index.readline()
                i_dic = json.loads(i_dic)
            # Colocamos las frecuencias. Recuerde que el IDF se calcula en la misma función de similitud
            for k in i_dic.keys():
                if k not in inverted_index.keys():
                    inverted_index[k] = i_dic[k]
                else:
                    # Se fusionan los vectores
                    inverted_index[k]['TF'] += (i_dic[k]['TF'])
                    # Se suman las cantidades
                    inverted_index[k]['DF'] += (i_dic[k]['DF'])
            os.remove(f_name)
        # Escribimos el índice completo
        json_file = open('resources/i_index.json', 'a', newline='\n', encoding='utf8')
        json_file.truncate(0)
        json_file.write(json.dumps(inverted_index, ensure_ascii=False, default=str))
        return

    # :: Define las particiones y, usando las funciones anteriores, construye el índice
    def BSB_index_construction(self):
        block = 500
        self.__build_inverted_index(size_tweets, block)
        self.__merge()
        return

# @Input: La frase de búsqueda, la cantidad de tweets a recuperar (k) y la cantidad total de tweets (n).
# @Return: Los k tweets más relevantes según el scoring de coseno.
def process_query(query, k, n):
    tokens = tp.tokenize(query)
    norm = len(tokens)

    # Obtenemos la frecuencia de las palabras en la query
    query_words = {}

    for q in tokens:
        if query_words.get(q) is None:
            query_words[q] = 1
        else:
            query_words[q] += 1

    # Leemos el index desde un archivo (memoria secundaria)
    tweets = {}
    with open('resources/i_index.json', "r") as index:
        i_dic = index.readline()
        i_dic = json.loads(i_dic)
    with open('resources/lengths.json', "r") as lens:
        lengths = lens.readline()
        lengths = json.loads(lengths)

    # Calculamos la distancia de coseno:
    for q in query_words:
        if i_dic.get(q) is not None:
            i = i_dic[q]
            # Normalizamos localmente el vector de palabras en el query
            qq = round((1 + math.log10(query_words[q])) * (math.log10(n / i['DF'])) / norm, 4)
            for tweet in i['TF']:
                # Sumamos a un documento el puntaje que va consiguiendo (con dist. de coseno)
                cosine = round(tweet[0] * (math.log10(n / i['DF'])) / lengths[str(tweet[1])] * qq, 4)
                if tweets.get(tweet[1]) is None:
                    tweets[tweet[1]] = cosine
                else:
                    tweets[tweet[1]] += cosine
    heap = []

    # Debemos hacer una segunda pasada porque los cosenos podrían haberse modificado...
    for tweet in tweets:
        heappush(heap, (-1 * tweets[tweet], tweet))

    # Uso de fila de prioridades para el ranking
    retrieved = {}
    for i in range(min(k, len(tweets))):
        retrieved[heap[0][1]] = -1 * heap[0][0]
        heappop(heap)
        heapify(heap)
    return retrieved

# Cambiar la temática de los tweets
def change_index_theme(keyword, maxtweets):
    global size_tweets
    size_tweets = maxtweets
    index_creator = creator_index()
    index_creator.make_new_index(keyword, maxtweets)
    ii = InvertedIndex()
    ii.BSB_index_construction()
    return

# Procesar consulta
def do_query(q, qns):
    rpta = process_query(q, qns, size_tweets)
    indexs = open("resources/index.txt", "r")
    tweets = open("resources/data.json", "r")
    jsondic = []
    json_file = open('static/data/rpta.json', 'a', newline='\n', encoding='utf8')
    json_file.truncate(0)
    for n in rpta:
        indexs.seek(0, 0)
        tweets.seek(0, 0)
        indexs.read((n-1)*10)
        line_tweet1 = int(indexs.read(10))
        line_tweet2 = int(indexs.read(10))
        tweets.read(line_tweet1)
        json_line = tweets.read(line_tweet2 - line_tweet1-1)
        json_line = json.loads(json_line)
        json_line['score'] = rpta[n]
        jsondic.append(json_line)
    json_file.write(json.dumps(jsondic, indent=6, ensure_ascii=False))
    return
