# Go-Back-N (GBN) over UDP

Este projeto implementa o protocolo de transferência confiável de dados **Go-Back-N (GBN)** sobre a camada de transporte **UDP**, utilizando a linguagem **Java**. O sistema simula de forma controlada perdas de pacotes na rede para demonstrar os mecanismos de controle de fluxo, retransmissão por timeout, controle de erros (checksum/SHA-1) e janelas deslizantes.

##  Estrutura do Projeto

A árvore de diretórios do projeto está organizada da seguinte forma:

```text
.
├── src/
│   ├── Emissor.java
│   └── Receptor.java
└── testes/
    ├── flor.jpeg
    └── trabalho.pdf

    src/: Contém o código-fonte Java das aplicações.

    testes/: Pasta contendo arquivos de diferentes extensões e tamanhos prontos para serem utilizados nos testes de transferência.

## Pré-requisitos

    Java Development Kit (JDK) instalado (versão 8 ou superior, recomendado JDK 17+).

    Terminal/Prompt de Comando com os comandos javac e java configurados nas variáveis de ambiente.

## Como Executar o Projeto

Para executar o sistema, abra dois terminais distintos na raiz do projeto (no mesmo nível das pastas src/ e testes/).
Passo 1: Compilar os Arquivos Java

Em qualquer um dos terminais, execute o comando abaixo para compilar ambas as classes dentro do pacote src:

javac src/Receptor.java src/Emissor.java

Passo 2: Iniciar o Receptor

O receptor deve ser iniciado primeiro para ficar aguardando conexões na porta padrão 5000. No terminal do Receptor, execute:

java src.Receptor

Passo 3: Iniciar o Emissor

No terminal do Emissor, execute o programa passando os parâmetros obrigatórios de configuração:

java src.Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda>

Exemplos Práticos de Execução

Exemplo 1: Enviando a imagem flor.jpeg com janela de tamanho 4 e 10% de perda simulada:
Bash

java src.Emissor testes/flor.jpeg localhost:testes/flor_recebida.jpeg 4 0.10

Exemplo 2: Enviando o documento trabalho.pdf com janela de tamanho 8 e sem perdas (0%):
Bash

java src.Emissor testes/trabalho.pdf localhost:testes/trabalho_final.pdf 8 0.00

## Detalhes da Arquitetura e Implementação

O protocolo utiliza um formato de datagrama personalizado para encapsular as mensagens de controle e dados.
Formato do Cabeçalho (Header)

O cabeçalho possui 11 bytes fixos, alocados via ByteBuffer com a seguinte estrutura:

    Tipo do Pacote (1 byte): Define o propósito do segmento (0: DATA, 1: ACK, 2: HANDSHAKE, 3: FIN, 4: ACK_HS, 5: ACK_FIN).

    Número de Sequência (4 bytes / int): Identifica o índice do pacote enviado.

    Número de ACK (4 bytes / int): Utilizado pelo receptor para indicar o último pacote recebido corretamente (ACK cumulativo).

    Tamanho do Payload (2 bytes / short): Especifica o tamanho útil dos dados anexados.

## Principais Mecanismos

    Handshake Confiável: Antes do envio do arquivo, o Emissor envia os metadados do arquivo (nome, tamanho e taxa de perda). O processo é idempotente; o emissor tenta até 5 vezes caso ocorra perda do pacote inicial ou do ACK_HS.

    Janela Deslizante (Sliding Window): O Emissor gerencia o fluxo enviando múltiplos pacotes simultâneos até atingir o limite definido por <tamanho_janela>.

    Timer Único e Watchdog: Um timer único associado ao pacote mais antigo não confirmado (base) roda em uma Thread separada no Emissor. Se disparar o Timeout (1 segundo), toda a janela ativa a partir da base é retransmitida.

    ACK Cumulativo: O Receptor aceita apenas pacotes estritamente em ordem (seq == expectedseqnum). Se receber pacotes fora de ordem ou duplicados, descarta-os e reenvia o ACK do último pacote correto gravado.

    Encerramento Elegante (FIN/ACK_FIN): Ao fim da leitura do arquivo, o Emissor despacha um sinal FIN. O Receptor encerra sua máquina de estados, calcula a integridade do arquivo e devolve um ACK_FIN contendo o hash do arquivo gravado.

    Verificação de Integridade (SHA-1): Ambas as pontas calculam o hash SHA-1 do arquivo. O Emissor compara visualmente o hash gerado na origem com o hash retornado pelo Receptor no fechamento para atestar que a transferência foi 100% íntegra.

## Estatísticas Geradas

Ao término de uma transferência bem-sucedida, as aplicações exibem um relatório detalhado no console:

    Emissor: Quantidade de pacotes originais, total de datagramas enviados, número de retransmissões disparadas por timeout, tempo total da operação e Throughput estimado em KB/s.

    Receptor: Pacotes gravados, pacotes fora de ordem descartados, perdas induzidas pela simulação, taxa de perda real observada e o Hash SHA-1 final.
