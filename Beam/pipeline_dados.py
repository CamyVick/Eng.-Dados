import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class CalcularTotalPorCategoria(beam.DoFn):
    def process(self, element):
        categoria, valor = element.split(',')
        return [(categoria, float(valor))]

options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    (
        p
        | "Ler arquivo" >> beam.io.ReadFromText(
            'input/vendas.csv',
            skip_header_lines=1,
            coder=beam.coders.coders.StrUtf8Coder()
        )
        | "Converter encoding" >> beam.Map(lambda line: line.encode('latin1').decode('utf-8', errors='ignore'))  
        | "Converter para tuplas" >> beam.ParDo(CalcularTotalPorCategoria())
        | "Somar por categoria" >> beam.CombinePerKey(sum)
        | "Formatar saída" >> beam.Map(lambda kv: f"{kv[0]},{kv[1]}")
        | "Escrever CSV" >> beam.io.WriteToText('output/total_por_categoria', file_name_suffix='.csv')
    )
