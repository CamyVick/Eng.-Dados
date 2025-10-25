"""
═══════════════════════════════════════════════════════════════════════════════
APACHE BEAM MASTERCLASS - GUIA DEFINITIVO
═══════════════════════════════════════════════════════════════════════════════

ÍNDICE:
1. Fundamentos Teóricos e Arquitetura
2. Modelo de Programação: PCollections e Transforms
3. Anatomia de um Pipeline Beam
4. DoFn: A Transform Mais Poderosa
5. Aggregations: GroupByKey vs CombinePerKey
6. Windowing: Processamento Temporal
7. I/O Connectors: Leitura e Escrita
8. Padrões de Design e Best Practices
9. Performance e Otimização
10. Casos de Uso Reais

Autor: Especialista em Engenharia de Dados
Data: 2025
═══════════════════════════════════════════════════════════════════════════════
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms import window, trigger
from apache_beam.transforms.combiners import Mean, Count, Top
import logging
from typing import Tuple, List, Dict, Any, Iterable
from datetime import datetime
import json


# ═══════════════════════════════════════════════════════════════════════════
# 1. FUNDAMENTOS TEÓRICOS
# ═══════════════════════════════════════════════════════════════════════════

"""
CONCEITO FUNDAMENTAL: Apache Beam = "Batch + Stream"

┌─────────────────────────────────────────────────────────────┐
│  ARQUITETURA DO BEAM (3 CAMADAS)                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. BEAM SDK (Python, Java, Go)                             │
│     └─> Você escreve o código aqui                          │
│                                                             │
│  2. BEAM MODEL (Abstração Unificada)                        │
│     └─> PCollections, Transforms, Windows, Triggers         │
│                                                             │
│  3. RUNNERS (Execution Engines)                             │
│     ├─> DirectRunner (local, debug)                         │
│     ├─> DataflowRunner (Google Cloud, produção)             │
│     ├─> FlinkRunner (Apache Flink)                          │
│     ├─> SparkRunner (Apache Spark)                          │ 
│     └─> SamzaRunner (Apache Samza)                          │
│                                                             │
└─────────────────────────────────────────────────────────────┘

DIFERENÇA BATCH vs STREAMING:

┌──────────────────┬──────────────────────────────────────────┐
│     BATCH        │          STREAMING                       │
├──────────────────┼──────────────────────────────────────────┤
│ Dados finitos    │ Dados infinitos                          │
│ Processa tudo    │ Processa continuamente                   │
│ Latência alta    │ Latência baixa                           │
│ Ex: CSV, DB      │ Ex: Kafka, PubSub, WebSocket             │
└──────────────────┴──────────────────────────────────────────┘

BEAM RESOLVE ISSO COM:
- Windowing: Dividir stream infinito em janelas finitas
- Watermarks: Rastrear progresso temporal dos dados
- Triggers: Controlar quando emitir resultados
"""


# ═══════════════════════════════════════════════════════════════════════════
# 2. PCOLLECTIONS: O DNA DO BEAM
# ═══════════════════════════════════════════════════════════════════════════

"""
PCOLLECTION = Parallel Collection

CARACTERÍSTICAS:
1. IMUTÁVEL: Uma vez criada, não muda (funcional programming)
2. DISTRIBUÍDA: Dados espalhados por múltiplas máquinas
3. NÃO-ORDENADA: Não garante ordem (a menos que você force)
4. TIPADA: Cada elemento tem um tipo (inferido ou explícito)

┌─────────────────────────────────────────────────────────┐
│  PCollection<String>                                    │
│  ┌───────┐  ┌───────┐  ┌───────┐  ┌───────┐             │
│  │ "abc" │  │ "def" │  │ "ghi" │  │ "jkl" │  ...        │
│  └───────┘  └───────┘  └───────┘  └───────┘             │
│                                                         │
│  Distribuída em N workers/máquinas                      │
└─────────────────────────────────────────────────────────┘

CRIANDO PCOLLECTIONS:
"""

def exemplo_criacao_pcollections():
    """Todas as formas de criar PCollections"""
    
    with beam.Pipeline() as p:
        
        # ═══════════════════════════════════════════════════════
        # MÉTODO 1: Create (in-memory) - Para testes/exemplos
        # ═══════════════════════════════════════════════════════
        
        pcoll_simples = p | "Criar lista" >> beam.Create([1, 2, 3, 4, 5])
        
        pcoll_tuplas = p | "Criar tuplas" >> beam.Create([
            ('João', 25),
            ('Maria', 30),
            ('Pedro', 28)
        ])
        
        # ═══════════════════════════════════════════════════════
        # MÉTODO 2: ReadFromText - Arquivos texto
        # ═══════════════════════════════════════════════════════
        
        linhas = p | "Ler arquivo" >> ReadFromText(
            'input/*.txt',           # Glob pattern
            skip_header_lines=1,      # Pular cabeçalho
            coder=beam.coders.StrUtf8Coder()  # Encoding
        )
        
        # ═══════════════════════════════════════════════════════
        # MÉTODO 3: Outros I/O Connectors
        # ═══════════════════════════════════════════════════════
        
        # BigQuery
        from apache_beam.io.gcp.bigquery import ReadFromBigQuery
        bq_data = p | ReadFromBigQuery(
            query='SELECT * FROM `project.dataset.table`',
            use_standard_sql=True
        )
        
        # Parquet
        from apache_beam.io.parquetio import ReadFromParquet
        parquet_data = p | ReadFromParquet('data/*.parquet')
        
        # Pub/Sub (streaming)
        from apache_beam.io.gcp.pubsub import ReadFromPubSub
        stream = p | ReadFromPubSub(topic='projects/my-project/topics/my-topic')


# ═══════════════════════════════════════════════════════════════════════════
# 3. TRANSFORMS: O CORAÇÃO DO BEAM
# ═══════════════════════════════════════════════════════════════════════════

"""
TRANSFORMS = Operações que transformam PCollections

HIERARQUIA:
    PTransform (classe base)
        ├─> Map (1:1)
        ├─> FlatMap (1:N)
        ├─> Filter (1:0 ou 1:1)
        ├─> ParDo (mais flexível)
        ├─> GroupByKey (agregação)
        ├─> CombinePerKey (agregação otimizada)
        ├─> Flatten (união)
        ├─> Partition (divisão)
        └─> ... e muitas outras

SINTAXE:
    output_pcoll = input_pcoll | "Nome da operação" >> Transform()
                                  └─── Label (obrigatório)
"""


class TransformsBasicas:
    """Demonstração completa de todas as transforms básicas"""
    
    @staticmethod
    def map_exemplo():
        """
        MAP: Transformação 1:1
        ════════════════════════
        
        Cada elemento de entrada produz EXATAMENTE 1 elemento de saída.
        
        Quando usar:
        - Converter tipos (string -> int)
        - Aplicar função simples
        - Extrair campos
        
        Performance: O(n) - linear
        """
        with beam.Pipeline() as p:
            
            # ─────────────────────────────────────────────────
            # Exemplo 1: Lambda simples
            # ─────────────────────────────────────────────────
            numeros = p | beam.Create([1, 2, 3, 4, 5])
            dobrados = numeros | "Dobrar" >> beam.Map(lambda x: x * 2)
            # Input:  [1, 2, 3, 4, 5]
            # Output: [2, 4, 6, 8, 10]
            
            # ─────────────────────────────────────────────────
            # Exemplo 2: Função nomeada (melhor para lógica complexa)
            # ─────────────────────────────────────────────────
            def processar_log(linha: str) -> Dict[str, Any]:
                """
                Parseia linha de log.
                Mais legível e testável que lambda.
                """
                partes = linha.split('|')
                return {
                    'timestamp': partes[0],
                    'nivel': partes[1],
                    'mensagem': partes[2]
                }
            
            logs = p | beam.Create([
                '2025-01-01 10:00:00|INFO|Sistema iniciado',
                '2025-01-01 10:00:05|ERROR|Falha na conexão'
            ])
            logs_parseados = logs | "Parsear logs" >> beam.Map(processar_log)
            
            # ─────────────────────────────────────────────────
            # Exemplo 3: Map com argumentos adicionais
            # ─────────────────────────────────────────────────
            def multiplicar(x: int, fator: int) -> int:
                return x * fator
            
            triplicados = numeros | "Triplicar" >> beam.Map(
                multiplicar,
                fator=3  # Argumento adicional
            )
    
    @staticmethod
    def flatmap_exemplo():
        """
        FLATMAP: Transformação 1:N
        ═══════════════════════════
        
        Cada elemento de entrada produz 0, 1 ou MÚLTIPLOS elementos de saída.
        
        Quando usar:
        - Dividir strings (split)
        - Explodir arrays
        - Gerar múltiplos eventos
        
        Performance: O(n*m) onde m = média de outputs por input
        """
        with beam.Pipeline() as p:
            
            # ─────────────────────────────────────────────────
            # Exemplo 1: Dividir frases em palavras
            # ─────────────────────────────────────────────────
            frases = p | beam.Create([
                'Apache Beam é incrível',
                'Python é poderoso'
            ])
            
            palavras = frases | "Dividir" >> beam.FlatMap(lambda x: x.split())
            # Input:  ['Apache Beam é incrível', 'Python é poderoso']
            # Output: ['Apache', 'Beam', 'é', 'incrível', 'Python', 'é', 'poderoso']
            
            # ─────────────────────────────────────────────────
            # Exemplo 2: Explodir array JSON
            # ─────────────────────────────────────────────────
            def explodir_compras(pedido: Dict) -> Iterable[Dict]:
                """
                Transforma 1 pedido com N itens em N linhas.
                
                Input: {'pedido_id': 1, 'itens': ['A', 'B', 'C']}
                Output: [
                    {'pedido_id': 1, 'item': 'A'},
                    {'pedido_id': 1, 'item': 'B'},
                    {'pedido_id': 1, 'item': 'C'}
                ]
                """
                for item in pedido['itens']:
                    yield {
                        'pedido_id': pedido['pedido_id'],
                        'item': item
                    }
            
            pedidos = p | beam.Create([
                {'pedido_id': 1, 'itens': ['A', 'B']},
                {'pedido_id': 2, 'itens': ['C', 'D', 'E']}
            ])
            
            itens_explodidos = pedidos | "Explodir" >> beam.FlatMap(explodir_compras)
            
            # ─────────────────────────────────────────────────
            # Exemplo 3: Gerar múltiplos eventos (denormalização)
            # ─────────────────────────────────────────────────
            def gerar_eventos(venda: Dict) -> Iterable[Tuple[str, Dict]]:
                """
                Gera múltiplos eventos de uma venda.
                Útil para criar diferentes visões dos dados.
                """
                # Evento por produto
                yield ('produto', {
                    'produto_id': venda['produto_id'],
                    'quantidade': venda['quantidade']
                })
                
                # Evento por cliente
                yield ('cliente', {
                    'cliente_id': venda['cliente_id'],
                    'valor_gasto': venda['valor']
                })
                
                # Evento por loja
                yield ('loja', {
                    'loja_id': venda['loja_id'],
                    'valor_vendido': venda['valor']
                })
    
    @staticmethod
    def filter_exemplo():
        """
        FILTER: Filtrar elementos
        ═══════════════════════════
        
        Mantém apenas elementos que satisfazem uma condição.
        
        Quando usar:
        - Remover dados inválidos
        - Selecionar subset
        - Limpar dados
        
        Performance: O(n)
        """
        with beam.Pipeline() as p:
            
            numeros = p | beam.Create([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            
            # ─────────────────────────────────────────────────
            # Exemplo 1: Filtro simples
            # ─────────────────────────────────────────────────
            pares = numeros | "Pares" >> beam.Filter(lambda x: x % 2 == 0)
            # Output: [2, 4, 6, 8, 10]
            
            # ─────────────────────────────────────────────────
            # Exemplo 2: Filtro complexo com função
            # ─────────────────────────────────────────────────
            def eh_valido(registro: Dict) -> bool:
                """
                Validação complexa de dados.
                Retorna True se registro é válido.
                """
                return (
                    registro.get('valor', 0) > 0 and
                    registro.get('cliente_id') is not None and
                    len(registro.get('produto', '')) > 0
                )
            
            vendas = p | beam.Create([
                {'valor': 100, 'cliente_id': 1, 'produto': 'A'},
                {'valor': -50, 'cliente_id': 2, 'produto': 'B'},  # Inválido
                {'valor': 200, 'cliente_id': None, 'produto': 'C'},  # Inválido
            ])
            
            vendas_validas = vendas | "Validar" >> beam.Filter(eh_valido)


# ═══════════════════════════════════════════════════════════════════════════
# 4. DOFN: A TRANSFORM MAIS PODEROSA
# ═══════════════════════════════════════════════════════════════════════════

"""
DoFn = "Do Function"

É a transform mais flexível. Permite:
✓ Múltiplas saídas (tagged outputs)
✓ Contexto e estado
✓ Side inputs
✓ Lifecycle hooks (setup, teardown)
✓ Métricas customizadas
✓ Tratamento de erros avançado

LIFECYCLE DE UM DoFn:

┌─────────────────────────────────────────────────────────────┐
│  WORKER 1                    WORKER 2                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  setup() ───┐                setup() ───┐                   │
│             │                            │                  │
│  ┌──────────▼──────────┐    ┌───────────▼──────────┐        │
│  │  start_bundle()     │    │  start_bundle()      │        │
│  │  process(elem1)     │    │  process(elem4)      │        │
│  │  process(elem2)     │    │  process(elem5)      │        │
│  │  process(elem3)     │    │  process(elem6)      │        │
│  │  finish_bundle()    │    │  finish_bundle()     │        │
│  └──────────┬──────────┘    └───────────┬──────────┘        │
│             │                            │                  │
│  teardown() ┘                teardown() ┘                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘

QUANDO USAR CADA MÉTODO:
- setup():         Conexões DB, carregar modelos ML, inicialização pesada
- start_bundle():  Contadores locais, batch de escritas
- process():       Lógica principal de processamento
- finish_bundle(): Flush de buffers, enviar batch acumulado
- teardown():      Fechar conexões, liberar recursos
"""


class DoFnAvancado(beam.DoFn):
    """DoFn com todos os recursos avançados"""
    
    def __init__(self, threshold: float):
        """
        Construtor: Executado uma vez quando o pipeline é construído.
        Use para parâmetros de configuração (não para estado mutável).
        """
        super().__init__()
        self.threshold = threshold
        
        # ⚠️ IMPORTANTE: Não inicialize recursos pesados aqui
        # (conexões, modelos, etc). Use setup() para isso.
        self.conexao_db = None  # Será inicializado no setup()
    
    def setup(self):
        """
        Executado UMA VEZ por worker, antes de processar qualquer elemento.
        
        Use para:
        - Abrir conexões com bancos de dados
        - Carregar modelos de ML
        - Inicializar clientes API
        - Qualquer operação cara que deve ser feita uma vez por worker
        """
        logging.info(f"[SETUP] Inicializando worker {id(self)}")
        
        # Exemplo: Conectar ao banco
        # self.conexao_db = psycopg2.connect(...)
        
        # Exemplo: Carregar modelo ML
        # self.modelo = tensorflow.keras.models.load_model('modelo.h5')
        
        # Inicializar métricas
        self.total_processado = beam.metrics.Metrics.counter(
            'meu_namespace', 'total_elementos'
        )
        self.erros = beam.metrics.Metrics.counter(
            'meu_namespace', 'total_erros'
        )
    
    def start_bundle(self):
        """
        Executado no INÍCIO de cada bundle (lote de elementos).
        
        Um bundle é um lote de elementos processados juntos.
        Tipicamente há muitos bundles por worker.
        
        Use para:
        - Inicializar contadores locais
        - Preparar buffer de escrita em batch
        - Resetar estado temporário
        """
        logging.info(f"[START_BUNDLE] Iniciando novo bundle")
        
        # Buffer para batch de escritas
        self.buffer_escrita = []
        self.bundle_contador = 0
    
    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        """
        Executado para CADA elemento.
        
        Esta é a função principal onde você processa os dados.
        
        Args:
            element: O elemento atual sendo processado
            timestamp: Timestamp do elemento (opcional)
            window: Janela do elemento (opcional)
        
        Yields:
            0, 1 ou múltiplos elementos
        """
        self.bundle_contador += 1
        self.total_processado.inc()
        
        try:
            # ─────────────────────────────────────────────────
            # Lógica de processamento
            # ─────────────────────────────────────────────────
            valor = float(element.get('valor', 0))
            
            if valor > self.threshold:
                # Output principal
                yield {
                    'elemento': element,
                    'timestamp': timestamp,
                    'window': window,
                    'status': 'aprovado'
                }
            else:
                # Output secundário (tagged output)
                yield beam.pvalue.TaggedOutput('rejeitados', element)
            
            # ─────────────────────────────────────────────────
            # Batch processing: Acumular para escrita
            # ─────────────────────────────────────────────────
            self.buffer_escrita.append(element)
            
            if len(self.buffer_escrita) >= 100:
                # Escrever batch
                # self.conexao_db.executemany(query, self.buffer_escrita)
                self.buffer_escrita.clear()
        
        except Exception as e:
            # ─────────────────────────────────────────────────
            # Tratamento de erro
            # ─────────────────────────────────────────────────
            self.erros.inc()
            logging.error(f"Erro ao processar {element}: {e}")
            
            # Emitir para dead letter queue
            yield beam.pvalue.TaggedOutput('erros', {
                'elemento': element,
                'erro': str(e),
                'timestamp': timestamp
            })
    
    def finish_bundle(self):
        """
        Executado no FIM de cada bundle.
        
        Use para:
        - Flush de buffers pendentes
        - Enviar dados acumulados
        - Logging de estatísticas do bundle
        """
        logging.info(f"[FINISH_BUNDLE] Bundle finalizado. Processados: {self.bundle_contador}")
        
        # Flush do buffer restante
        if self.buffer_escrita:
            # self.conexao_db.executemany(query, self.buffer_escrita)
            self.buffer_escrita.clear()
    
    def teardown(self):
        """
        Executado UMA VEZ por worker ao finalizar.
        
        Use para:
        - Fechar conexões
        - Liberar recursos
        - Cleanup final
        """
        logging.info(f"[TEARDOWN] Finalizando worker {id(self)}")
        
        if self.conexao_db:
            self.conexao_db.close()


def exemplo_dofn_uso():
    """Como usar DoFn com múltiplas saídas"""
    
    with beam.Pipeline() as p:
        
        dados = p | beam.Create([
            {'id': 1, 'valor': 100},
            {'id': 2, 'valor': 50},
            {'id': 3, 'valor': 200},
        ])
        
        # Processar com múltiplas saídas
        resultado = dados | "Processar" >> beam.ParDo(
            DoFnAvancado(threshold=75)
        ).with_outputs('rejeitados', 'erros', main='aprovados')
        
        # Acessar cada saída
        aprovados = resultado.aprovados
        rejeitados = resultado.rejeitados
        erros = resultado.erros
        
        # Processar cada saída separadamente
        aprovados | "Salvar aprovados" >> WriteToText('output/aprovados')
        rejeitados | "Salvar rejeitados" >> WriteToText('output/rejeitados')
        erros | "Salvar erros" >> WriteToText('output/erros')


# ═══════════════════════════════════════════════════════════════════════════
# 5. AGGREGATIONS: GROUPBYKEY vs COMBINEPERKEY
# ═══════════════════════════════════════════════════════════════════════════

"""
AGREGAÇÕES: Como juntar/combinar dados

┌──────────────────────────────────────────────────────────────────┐
│  GROUPBYKEY vs COMBINEPERKEY                                     │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Input: [('A', 1), ('B', 2), ('A', 3), ('B', 4)]                 │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐      │
│  │ GroupByKey()                                           │      │
│  │ - Agrupa TODOS os valores na memória                   │      │
│  │ - Não faz processamento intermediário                  │      │
│  │ - Pode causar OOM (Out of Memory)                      │      │
│  │                                                        │      │
│  │ Output: [('A', [1, 3]), ('B', [2, 4])]                 │      │
│  └────────────────────────────────────────────────────────┘      │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐      │
│  │ CombinePerKey(sum)                                     │      │
│  │ - Combina valores INCREMENTALMENTE                     │      │
│  │ - Processamento distribuído eficiente                  │      │
│  │ - Muito mais eficiente em memória                      │      │
│  │                                                        │      │
│  │ Output: [('A', 4), ('B', 6)]                           │      │
│  └────────────────────────────────────────────────────────┘      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘

REGRA DE OURO:
✓ Use CombinePerKey quando possível (sum, count, mean, max, etc)
✗ Use GroupByKey apenas quando realmente precisa de todos os valores
"""


class AgregacoesAvancadas:
    """Demonstração completa de agregações"""
    
    @staticmethod
    def groupbykey_exemplo():
        """
        GroupByKey: Agrupa valores por chave
        ═════════════════════════════════════
        
        CUIDADO: Mantém TODOS os valores na memória!
        Use apenas quando realmente precisa da lista completa.
        """
        with beam.Pipeline() as p:
            
            vendas = p | beam.Create([
                ('Livros', 100),
                ('Eletrônicos', 250),
                ('Livros', 75),
                ('Eletrônicos', 300),
                ('Livros', 120),
            ])
            
            # GroupByKey
            agrupado = vendas | "Agrupar" >> beam.GroupByKey()
            # Output: [
            #   ('Livros', [100, 75, 120]),
            #   ('Eletrônicos', [250, 300])
            # ]
            
            # Agora você precisa processar manualmente
            def calcular_estatisticas(kv):
                categoria, valores = kv
                lista_valores = list(valores)  # ⚠️ Materializa na memória
                return {
                    'categoria': categoria,
                    'total': sum(lista_valores),
                    'media': sum(lista_valores) / len(lista_valores),
                    'count': len(lista_valores),
                    'max': max(lista_valores),
                    'min': min(lista_valores)
                }
            
            stats = agrupado | "Calcular" >> beam.Map(calcular_estatisticas)
    
    @staticmethod
    def combineperkey_exemplo():
        """
        CombinePerKey: Agrega eficientemente
        ══════════════════════════════════════
        
        MUITO mais eficiente! Combina parcialmente antes de agrupar.
        """
        with beam.Pipeline() as p:
            
            vendas = p | beam.Create([
                ('Livros', 100),
                ('Eletrônicos', 250),
                ('Livros', 75),
                ('Eletrônicos', 300),
                ('Livros', 120),
            ])
            
            # ─────────────────────────────────────────────────
            # Funções de combinação built-in
            # ─────────────────────────────────────────────────
            
            total = vendas | "Soma" >> beam.CombinePerKey(sum)
            # Output: [('Livros', 295), ('Eletrônicos', 550)]
            
            contagem = vendas | "Contar" >> beam.combiners.Count.PerKey()
            # Output: [('Livros', 3), ('Eletrônicos', 2)]
            
            media = vendas | "Média" >> beam.combiners.Mean.PerKey()
            # Output: [('Livros', 98.33), ('Eletrônicos', 275)]
            
            top3 = vendas | "Top 3" >> beam.combiners.Top.PerKey(3)
            # Output: [('Livros', [120, 100, 75]), ...]
    
    @staticmethod
    def combiner_customizado():
        """
        CombineFn customizado para agregações complexas
        ═════════════════════════════════════════════════
        
        Quando as funções built-in não são suficientes.
        """
        
        class EstatisticasCompletas(beam.CombineFn):
            """
            CombineFn para calcular múltiplas estatísticas eficientemente."""