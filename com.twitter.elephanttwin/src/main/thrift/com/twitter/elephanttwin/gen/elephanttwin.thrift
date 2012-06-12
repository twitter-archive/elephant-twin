namespace java com.twitter.elephanttwin.gen

/*
  Need better doctypes...
*/
enum DocType {
  RECORD = 1
  BLOCK = 2
}

/**
We can extend this in the future to other mechanisms
such as MapFiles, TFiles, HBase, etc.
**/
enum IndexType {
  LUCENE = 1
  MAPFILE = 2
}

/**
We may need to add more fields here. Jimmy?
**/
struct IndexedField {
  1: string fieldName
  2: bool stored
  3: bool indexed
  4: bool analyzed
}

/**
This is a representation of org.apache.hadoop.fs.FileChecksum
**/
struct FileChecksum {
  1: string algorithmName
  2: binary bytes
  3: i32 length
}

struct FileIndexDescriptor {
  1: DocType docType
  2: IndexType indexType 
  3: i32 indexVersion
  4: string sourcePath
  5: FileChecksum checksum
  6: list<IndexedField> indexedFields
}

/**
I suppose it's possible that we'll start indexing multiple files in one index, as well as 
potentially creating multiple indexes for one file.
**/
struct ETwinIndexDescriptor {
  1: list<FileIndexDescriptor> fileIndexDescriptors
  2: i32 indexPart                          # We can create several indexes for a collection of Files.
   # non-generic things like the lucene analyzer class 
   # and lucene similarity go in here
  3: optional map<string, string> options 

}
