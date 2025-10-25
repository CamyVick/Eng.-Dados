import pandas as pd
import random

# Listas base
nomes = [
    "Ana", "Bruno", "Carlos", "Daniela", "Eduardo", "Fernanda", "Gabriel", "Helena", "Igor", "Juliana",
    "Karen", "Lucas", "Mariana", "Nicolas", "Olivia", "Paulo", "Rafaela", "Sérgio", "Tatiane", "Vinícius"
]

# Siglas de estados brasileiros
regioes = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "DF", "AM", "PA", "GO", "MT", "MS"]

# Geração dos dados
dados = []
for _ in range(100):
    nome = random.choice(nomes)
    regiao = random.choice(regioes)
    idade = random.randint(18, 70)
    dados.append((nome, regiao, idade))

# Criação do DataFrame
df = pd.DataFrame(dados, columns=["nome", "regiao", "idade"])

# Exibe as primeiras linhas
print(df.head())

# (opcional) salvar CSV
df.to_csv("C:/Users/camil/Documents/EngDados/Pyspark/Arquivos/pessoas.csv", index=False, encoding="utf-8", sep=';')
