# beetle-thermal
Alignment the tracking path between and normal and thermal camera to get the temperature variance in moving

## Requirement

This project aims to get the temperature variance when the beetle moving. As so, we have to prepare some data as the input in this pipeline.

1. temperature per pixel per frame
2. beetles moving paths

### File Structure

```
data/
└── demo/
    ├── path/
    │   └── paths.csv
    └── temp/
        ├── 1.MAT
        ...
        └── n.MAT
```

> It might have multiple video folder under `path/` contains the `paths.csv` for each.

### Temperature file

**ThermalCAM software -> .SEQ -> .TXT**

Use the VBA script to extract temperature from seq file frame by frame

### Paths

**beetle-tracking pipeline -> paths.csv (per video)**

However, one experiment contains multiple videos, should be concatenated the paths by timestamp