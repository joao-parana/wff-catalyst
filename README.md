
# wff-catalyst

> OBS: este programa foi testado no macOS e no Linux com Apache Spark versões 2.2.1 e 2.3.0. A versão da JVM utilizada foi a 1.8 e o Scala utilizado foi a versão 2.11.11

## Clone o repositório no seu computador

Abra o Terminal no seu Sistema Operacional e execute:

```bash
git clone git@github.com:joao-parana/wff-catalyst.git
cd wff-catalyst
```

## Reprodução do experimento

Instale o Ammonite que é uma shell REPL para Scala com funcionalidades de edição usando apenas o terminal.

Com ele podemos carregar qualquer binário Java ou Scala sabendo apenas o group-id, artefact-id e a versão.

```bash
sudo curl -L -o /usr/local/bin/amm https://git.io/vdNvV
sudo chmod +x /usr/local/bin/amm 
```

Invoque o Ammonite

```bash
amm
```

Na shell amm digite:

```scala
import $ivy.`org.apache.spark::spark-sql:2.3.0`
```

Isto carrega o SparkSQL, o Catalyst e todas as dependências. 
Na primeira vez os JARs são baixados e armazenados no cache local e ficam disponíveis para uso. 

Em seguida execute na shell `amm` o seguinte:

```scala
import $file.map_pushdow, map_pushdow._
```

Isto executará o programa, exibirá na tela o log de execução e terminará a shell REPL voltando pro terminal do Linux / macOS.

### Como testar com versão em desenvolvimento do Spark 

Testando com versão local do Spark obtida via `git clone` seguido de execução do script de build.

Este é o caso, por exemplo, quando estiver testando numa versão beta do Spark.

Em 05/03/2018, para testar com a versão `2.4.0` do Spark é necessário fazer o build do Spark Localmente e executar:

```scala
import coursier.MavenRepository
interp.repositories() ++= Seq(MavenRepository("file:/Users/admin/.m2/repository"))
import $ivy.`org.apache.spark::spark-sql:2.4.0`
```

Alterar `"file:/Users/admin/.m2/repository"` de acordo com sua realidade no seu host,
apontando para a localização correta do repositorio local do Maven


