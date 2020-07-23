# Web Visualiser

[rfview.html](rfview.html) is a web program (run locally on your machine) where you can upload the json model produced by variantspark and it visualises trees in the model. You can identify which tree to be visualised. Node color and node labels could be set to different parameters such as number of samples in the node or the node impurity. It uses [vis.js](https://visjs.org/) for tree Visualisation.

you need to turn of cross origin in your browser

- For Google Chrome on Ubuntu run it from command line like this

```
google-chrome --allow-file-access-from-files
```

- For Safari om Mac:

1- Safari -> Preferences -> Advanced

2- Tick "Show Develop Menu"

3- In "Develop Menu", tick "Disable Cross-Origin Restrictions"
