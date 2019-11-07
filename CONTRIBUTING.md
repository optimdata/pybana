## Deploy package

```
$> rm -rf dist/
$> python setup.py sdist
$> python setup.py bdist_wheel
$> twine upload dist/*
```

## Implementation details

### Handling colors

Visualization colors can be handled with two mechanisms:
- **Per visualization**. There's a mapping `{label: color}` in `Visualization.uiStateJSON.vis.colors`. The label can be either a metric label or a group label.
- **Global**. There are defined the advanced settings (which is stored in the `Config` document). Check out the [docs](https://www.elastic.co/guide/en/kibana/current/advanced-options.html#kibana-visualization-settings) for more informations.

Then the choice of a color for group or metric is by order of priority:
- Pick the one defined in `uiStateJSON` if there is one
- Pick the one ine Config.visualization-settings
- Pick from the kibana palette `["#57c17b", "#6f87d8", "#663db8", "#bc52bc", "#9e3533", "#daa05d", "#00a69b"]` defined in `ui/public/vis/components/color/seed_colors.js`.

:warning: Mimicing how kibana chooses colors from the palette is not 100% iso. Help would be appreciated on this topic.
