# Proyecto_BD2_2

## Pasos del procesamiento de la query
### Crear el indice (por bloques)

### Tokenizar la query

Para la tokenización de la query hacemos uso de la librería nltk.
Donde usamos las funciones encode, decode, stem, tokenize. Luego,
procedemos a deshacernos de los stopwords con ayuda de la librería
y nuestra lista de caracteres a no incluir.
```python
    def tokenize(self, tweet):
        tweet = tweet.encode('ascii', 'ignore').decode('ascii')
        return [
            self.stemmer.stem(t) for t in self.tknzr.tokenize(tweet)
            if t not in stopwords.words('spanish') and
                t not in 
                ["<", ">", ",", "º", ":", ";", ".", "!", "¿", "?", ")", "(", "@", "'",'"','\"', '.', '...', '....']
        ]
```
### Aplicar cosenos

### Filtrar los mejores
