# Relatório em LaTeX

O arquivo `modelo_relatorio.tex` contém o relatório final em formato IEEE de
duas colunas. O documento tem três páginas, respeitando o limite de quatro
páginas, e lê os gráficos gerados em `../resultados/`. A cópia pronta para
entrega está em `../relatorio_projeto1_Caique_Pinheiro_Andrade.pdf`.

## Preparação no VS Code

Instale estas extensões:

1. **LaTeX Workshop** (`James-Yu.latex-workshop`) — compilação, visualização do
   PDF, referências e sincronização entre fonte e PDF.
2. **LTeX+** (`ltex-plus.vscode-ltex-plus`) — opcional, para revisão ortográfica
   e gramatical em português.

A extensão não inclui o compilador. Em Ubuntu/Debian, instale uma distribuição
TeX e o `latexmk`:

```bash
sudo apt update
sudo apt install latexmk texlive-latex-base texlive-latex-recommended \
  texlive-latex-extra texlive-fonts-recommended texlive-lang-portuguese \
  texlive-publishers
```

No Windows, uma opção simples é instalar **MiKTeX** e habilitar a instalação
automática de pacotes ausentes. No macOS, use **MacTeX**.

## Compilação

A partir da raiz do repositório:

```bash
cd relatorio
latexmk -pdf -interaction=nonstopmode -halt-on-error modelo_relatorio.tex
cp modelo_relatorio.pdf ../relatorio_projeto1_Caique_Pinheiro_Andrade.pdf
```

Para remover arquivos auxiliares:

```bash
latexmk -C modelo_relatorio.tex
```

No VS Code, abra `modelo_relatorio.tex`, execute **LaTeX Workshop: Build LaTeX
project** e depois **View LaTeX PDF** pela paleta de comandos. O recipe padrão
com `latexmk` é suficiente.

## Controle do limite

Após compilar, confira a quantidade de páginas:

```bash
pdfinfo relatorio/modelo_relatorio.pdf | grep Pages
```

Se o texto final exceder quatro páginas, primeiro reduza redundâncias e
legendas; não diminua artificialmente margens ou tamanho de fonte do padrão
IEEE.
