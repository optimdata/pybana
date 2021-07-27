## History

### 0.7.0

- Add support for top_hits
- Fix handle searches in dashboards

### 0.6.3

- Fix escaping of metric label

### 0.6.2

- Fix when value is null. Then value is ignored.

### 0.6.1

- Fix `ContextVisualization.is_duration_agg`

### 0.6.0

- Add support for duration formating for axes that represent a duration serie.

### 0.5.6

- Fix packaging

### 0.5.5

- Clip line when y-axis extent is set

### 0.5.4

- Support vega viz without data.url attribute
- Fix none type handling on datasweet eval

### 0.5.3

- Fix terms custom metric

### 0.5.2

- Fix nan values in bucket due to datasweet formula

### 0.5.1

- Fix case when a datasweet formula depends on other datasweet formula

### 0.5.0

- Add support for vega visualizations

### 0.4.2

- Fix Add support for terms sorting by custom metric

### 0.4.1

- Fix `format_from_interval` for week intervals

### 0.4.0

- Handle Category axe rotation

### 0.3.1

- Handle ZeroDivisionError in datasweet

### 0.3.0

- Rename `Context` to `Scope`
- Add `BaseDocument.json_attrs` to simplify parsing of some fields (ex: Dashboard.panelsJSON)
- Add datasweet support
- Add support for `using` in client

### 0.2.0

- Add `Search` model
- Add `VegaRenderer` and vega-cli

### 0.1.0

- First version
