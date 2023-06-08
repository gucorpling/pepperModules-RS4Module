# RS4Module 

This module is used to construct RST++ structures (signals and secondary edges).
It depends upon annotations created by the [RST importer module](https://github.com/korpling/pepperModules-RSTModules)
and should be run AFTER the merging manipulator has been run, if it is being used.

# Usage
Add a line like this to your `.pepperparams` file, *after* your `RSTImporter` module (required) and *after* `Merger` module (optional):


```xml
<!-- Read RS4 file, construct basic RST structures -->
<importer name="RSTImporter" path="rst_dev/bug_in_full/rst/GUM/">
  <!-- ... -->
</importer>
<!-- Merge RST graph with other graphs, if needed -->
<manipulator name="Merger" id="...">
  <!-- ... -->
</manipulator>
<!-- Use the temporary annotations created by RSTImporter to construct RS4 structures -->
<manipulator name="RS4" id="...">
</manipulator>
```


# Changelog
* 0.1.0 - Initial Release
