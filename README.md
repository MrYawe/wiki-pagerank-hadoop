# wiki-pagerank-hadoop

## Starting
### First version
This alpha version is based on this [blog article)(http://blog.xebia.com/wiki-pagerank-with-hadoop/).
- create an `input` folder under the project root with your xml file on it. You can download it here [here](https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-pages-articles.xml.bz2).
- run the `WikiPageRanking` class with Intellij.


## Goal
The main goal is to improve the pagerank algorithm and to use SQL files instead of the huge XML file.

The relevant SQL files are:
- `frwiki-latest-page.sql.gz`
- `frwiki-latest-pagelinks.sql.gz`

They are provided by Wikipedia [here](https://dumps.wikimedia.org/frwiki/latest).
