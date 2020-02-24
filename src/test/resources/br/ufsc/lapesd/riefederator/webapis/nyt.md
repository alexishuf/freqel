The `nyt_*.json` JSON files were obtained from inspection of network traffic in the official documentation pages. They contain a swagger 2.0 description packed within another API-like JSON. Useful testing parser tolerance.

The `nyt_*.yaml` YAML files were obtained from the same official documentation pages, but clicking on the "download spec" button. They are pretty-printed YAML files containing the swagger description as the root.

Official documentation:
- [Semantic API](https://developer.nytimes.com/docs/semantic-api-product/1/overview)
- [Books API](https://developer.nytimes.com/docs/semantic-api-product/1/overview)

The API reqires authentication through an API key. Creating a API key is (or was) free.