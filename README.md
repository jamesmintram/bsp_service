# bsp_service

A small project designed to work with the BSP viewer I am writing in Objective-C and Metal.

## Installation

None yet


### Bugs

...


[Segment1, Segment2, Segment3]


Handle a "Flush all" signal
Handle Filenaming (<NodeID>.<fileNumber>.segment.date.txt)

   /---- One Segment	-- Process -- OutBuffer -- S3 Writer
  /----- Two Segment	-- Process -- OutBuffer -- S3 Writer
-------- Three Segment	-- Process -- OutBuffer -- S3 Writer
  \----- Four Segment	-- Process -- OutBuffer -- S3 Writer
   \---- Five Segment	-- Process -- OutBuffer -- S3 Writer