# ann-search
Fast approximate nearest neighbour search based on Annoy

The purpose of this application is to add the following features on top of Annoy:
- Index rebuilds
- Splitting big index for fast writes, optimizing the number of splits in the background to improve searching speed
- Searching across multiple index files
