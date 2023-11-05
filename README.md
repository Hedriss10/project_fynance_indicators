# Projeto de Indicadores Forex usando Python

Este projeto tem como objetivo fornecer uma coleção de scripts Python para calcular diversos indicadores utilizados no mercado de Forex. Os indicadores incluídos no projeto são:

- Commodity Channel Index (CCI)
- Chaikin Money Flow (CMF)
- Ease of Movement (EFI)
- Índice de Força (IDF)
- Momentum
- Índice de Força Relativa (RSI)
- Estocástico
- Ultimate Oscillator (UO)

## Estrutura do Projeto

```
/backend
    /src
        - cci.py
        - cmf.py
        - efi.py
        - idf.py
        - momentum.py
        - rsi.py
        - stochastic.py
        - uo.py
    /data
        - 6A1.csv
        - 6E1.csv
        - BGI1.csv
        ...
/docs
/env
.gitignore
mkdocs.yml
README.md
```

A estrutura do projeto está organizada da seguinte forma:

- **`/backend`**: Contém os scripts Python para calcular os indicadores.
    - **`/src`**: Local onde os scripts individuais para cada indicador são armazenados.

- **`/data`**: Contém os arquivos CSV contendo os dados necessários para o cálculo dos indicadores.

- **`/docs`**: Documentação do projeto (não incluída no escopo atual).

- **`/env`**: Ambiente virtual Python (não incluído no escopo atual).

- **`.gitignore`**: Arquivo para especificar quais arquivos ou pastas devem ser ignorados pelo controle de versão.

- **`mkdocs.yml`**: Configurações para a geração da documentação (não incluída no escopo atual).

## Uso

Para utilizar os scripts de indicadores, siga os passos abaixo:

1. Certifique-se de ter o Python e as bibliotecas necessárias instaladas. Para instalar as dependências, utilize o comando:
   ```
   pip install -r requirements.txt
   ```

2. Execute o script correspondente ao indicador desejado a partir do diretório `/backend/src`.

3. Os resultados serão exibidos no console ou podem ser salvos em um arquivo, dependendo da implementação específica do indicador.

## Exemplo de Uso

Aqui está um exemplo de como usar o script para calcular o Commodity Channel Index (CCI):

```python
python cci.py
```

Isso irá calcular o CCI usando os dados do arquivo `6A1.csv` localizado em `/backend/data` e exibir o resultado no console.

## Contribuições

Contribuições são bem-vindas! Se você gostaria de adicionar novos indicadores, melhorar a eficiência dos códigos ou corrigir possíveis bugs, sinta-se à vontade para abrir um pull request.

## Licença

Este projeto está licenciado sob a [MIT License](LICENSE).
