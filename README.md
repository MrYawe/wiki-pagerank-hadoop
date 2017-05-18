# wiki-pagerank-hadoop

## Starting
1) Download these files:
- [page.sql.gz](https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-page.sql.gz)
- [pagelinks.sql.gz](https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-pagelinks.sql.gz)
2) Create the `input_pages` and `input_links` folders at the root of the project.
3) Put `frwiki-latest-page.sql.gz` in `input_pages` and `frwiki-latest-pagelinks.sql.gz` in `input_pagelinks`.
4) Download dependencies with `mvn install`
5) You can run the jar in the `target` folder with 3 args: "input_pagelinks input_pages final_result". The `final_result` folder will be created automatically and musn't exist at start.



