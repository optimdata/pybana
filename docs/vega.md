# Render visualization to vega spec

This package provide methods to render a `Visualization` to a vega specification.

## Why vega?

Vega was chosen as it provide a complex but almost exhaustive visualization grammar. Vega's api allows the rendering of visualizations both on the backend and frontend and has bridges with the main js frameworks (react, vue…).

## Usage

```python
from pybana import VegaTranslator, VegaRenderer, ElasticTranslator

# Let's assume you have a visualization & a context.
search = ElasticTranslator().translate(visualization, context)
response = search.execute()

# Translate to a vega spec
vega = VegaTranslator().translate(visualization, response, context)
```

## From vega to html

This package also provides a python helper to render a vega spec to html markup using a node subprocess. 

### Installation

To make it work, you need to:
- Install a recent version of node. So far, it has been tested using `v8.9.3`
- Install the [vega package](https://www.npmjs.com/package/vega).

### Usage

```python
from pybana import VegaRenderer

# Let's assume you have a vega spec

# Render it to a svg html node.
VegaRenderer().to_svg(vega)
```
