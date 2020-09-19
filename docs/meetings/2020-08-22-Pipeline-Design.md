# ECMWF Beam Pipeline Design Notes

*Attendees*: 
* alxr@
* celkin@

## Summary of Discussion

Carl and I (alxr@) discussed the pipeline API and lessons from a previous project's ECMWF downloader 
([go/download-ecmwf](http://go/download-ecmwf)).

**Q: How should the user specify what to download?**

Given the range of options available, we agreed it makes sense to structure input to the CLI as follows: 
- Users will pass in a `<client>` argument to choose between CDS, MARS, or any other clients we may support in the future.
- Next, a `<config>` argument, which takes a path/to/a/file.cfg, will allow users to specify exactly what data they'd like 
to get (region, features, etc.). Each config file will be specific to the particular client implementation. Ideally, the
file will mirror each client's native API spec as closely as possible. 
- The CLI should take a `<date_range>` as an argument, not in the config. It's important to partition data by time to 
implement parallelism in our pipeline. Thus, this should be a first-class argument.

Ideally, we'd like our tool to be as thin (or simple) of an interface as possible for interacting with each client API.

**Q: How should we parallelize the data?**

- Some weather APIs have limits to concurrent downloads / workers. It's recommended for one of the downloaders, we have
something like 15 workers, 3 downloading files at once.
- If we have too large a time-scale for each region, the file sizes will be really large and the download will take a long
time.
- Recommendation: Split the download by the day scale (for smaller payloads).

**Q: Who will (potentially) be users of this pipeline**

Likely, [GeoStore](https://g3doc.corp.google.com/geostore/base/proto/g3doc/overview.md?cl=head) and
[EarthEngine](http://go/earthengine) will be interested in this project.

## Raw Notes


Config file

Diving things up

- Mars divides across different tape drives. 
- Need to be one same tape drive to go faster
- Even still, downloading too many parameters in a month slowed things down
- if the size is greater than N bytes, the program should split up the download by day. 


The date range is part of the configuration. Make the date range is a 4th flag. 

Add help text in places, e.g. grid resolution.


Users: 
- Geostore -- John Platt
- Earth Engine (they should figure out how to get stiff in there).
