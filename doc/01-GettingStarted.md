## Think Stats - Getting Started

One of our new starters here at Metail was keen to brush up his
statistics, and it's more than 20 years since I completed an
introductory course at university so I knew I would benefit from some
revision. We also have a bunch of statisticians in the office who wnat
to brush up their Clojure, so I thought it might be fun to organise a
lunchtime study group to work through Allen Downey's
[Think Stats](http://shop.oreilly.com/product/0636920034094.do) and
attempt the exercises in Clojure.

We'll use Clojure's [Incanter](http://FIXME) library which provides
utilities for statistical analysis and generating charts. Create a
Leiningen project for our work:

    lein new thinkstats

Make sure the `project.clj` depends on Clojure `1.7.0` and add a
dependency on Incanter `1.5.6`:

    :dependencies [[org.clojure/clojure "1.7.0"]
                   [incanter "1.5.6"]]

We're using the second edition of the book which is available
[online in HTML format](http://greenteapress.com/thinkstats2/html/index.html),
and meeting on Wednesday lunchtimes to work through it together.

## Parsing the data

In the first chapter of the book, we are introduced to a data set from
the US Centers for Disease Control and Prevention, the National Survey
of Family Growth. The data are in a gzipped file with fixed-with
columns. An accopanying Stata dictionary describes the variable names,
types, and column idicces for each record. Our first job will be to
parse the dictionary file and use that information to build a parser
for the data.

I cloned the Github repository that accompanies Allen's book:

    git clone https://github.com/AllenDowney/ThinkStats2

Then created symlinks to the data files from my project:

    cd thinkstats
    mkdir data
    cd data
    for f in ../../ThinkStats2/{*.dat.gz,*.dct}; do ln -s $f; done

I can now read the Stata dictionary for the familiy growth study from
`data/2002FemPreg.dct`. The dictionary looks like:

    infile dictionary {
        _column(1)  str12    caseid     %12s  "RESPONDENT ID NUMBER"
        _column(13) byte     pregordr   %2f  "PREGNANCY ORDER (NUMBER)"
    }

If we skip the first and last lines of the dictionary, we can use a
regular expression to parse each column defitinion:

    (def dict-line-rx #"^\s+_column\((\d+)\)\s+(\S+)\s+(\S+)\s+%(\d+)(\S)\s+\"([^\"]+)\"")

We're capturing the column position, colum type, column name, format
and length, and description. Let's test this at the REPL. First we
have to read a line from the dictionary:

    (require '[clojure.java.io :as io])
    (def line (with-open [r (io/reader "data/2002FemPreg.dct")]
                (first (rest (line-seq r)))))

We use `rest` to skip the first line of the file then grab the first
column definition.  Now we can try matching this with our regular expression:

    (re-find dict-line-rx line)

This returns the string that matched and the capture groups we defined
in our regular expression:

    ["    _column(1)      str12                             caseid  %12s  \"RESPONDENT ID NUMBER\""
     "1"
     "str12"
     "caseid"
     "12"
     "s"
     "RESPONDENT ID NUMBER"]

We need to do some post-processing of this result to parse the column
index and length to integers; we'll also replace underscores in the
column name with hyphens, which makes for a more idiomatic Clojure
variable name. Let's wrap that up in a function:

    (require '[clojure.string :as str])

    (defn parse-dict-line
      [line]
      (let [[_ col type name f-len f-spec descr] (re-find dict-line-rx line)]
        {:col    (dec (Integer/parseInt col))
         :type   type
         :name   (str/replace name "_" "-")
         :f-len  (Integer/parseInt f-len)
         :f-spec f-spec
         :descr  descr}))

Note that we're also decrementing the column index - we need
zero-indexed column indices for Clojure's substring function. Now when
we parse our sample line we get:

    {:col 0,
     :type "str12",
     :name "caseid",
     :f-len 12,
     :f-spec "s",
     :descr "RESPONDENT ID NUMBER"}

With this function in hand, we can write a parser for the dictionary file:

    (defn read-dict-defn
      "Read a Stata dictionary file, return a vector of column definitions."
      [path]
      (with-open [r (io/reader path)]
        (mapv parse-dict-line (butlast (rest (line-seq r))))))

We use `rest` and `butlast` to skip the first and last lines of the
file, and `mapv` to force eager evaluation and ensure we process all
of the input before the reader is closed when we exit `with-open`.

    (def dict (parse-dict-defn "data/2002FemPreg.dat"))

We're going to use `subs` to extract the raw value of each column from
the data. This will give us a string, and the `:type` key we've
extracted from the dictionary tells us how to interpret this. We've
seen the types `str12` and `byte` above, but what other types appear
in the dictionary?

    (distinct (map :type dict))
    ;=> ("str12" "byte" "int" "float" "double")

We'll leave `str12` unchanged, coerce `byte` and `int` to `Long`, and
`float` and `double` to `Double`:

    (defn parse-value
      [type raw-value]
      (when (not (empty? raw-value))
        (case type
          ("str12")          raw-value
          ("byte" "int")     (Long/parseLong raw-value)
          ("float" "double") (Double/parseDouble raw-value))))

We can now build a record parser from the dictionary definition:

    (defn make-row-parser
      "Parse a row from a Stata data file according to the specification in `dict`.
       Return a vector of columns."
      [dict]
      (fn [row]
        (reduce (fn [accum {:keys [col type name f-len]}]
                  (let [raw-value (str/trim (subs row col (+ col f-len)))]
                    (conj accum (parse-value type raw-value))))
                []
                dict)))

To read gzipped data, we need to open an input stream, coerce this to
a `GZIPInputStream`, and construct a buffered reader from that. For
convenience, we'll define a function to do this automatically if
passed a path ending in `.gz`.

    (import 'java.util.zip.GZIPInputStream)

    (defn reader
      "Open path with io/reader; coerce to a GZIPInputStream if suffix is .gz"
      [path]
      (if (.endsWith path ".gz")
        (io/reader (GZIPInputStream. (io/input-stream path)))
        (io/reader path)))

Give a dictionary and reader, we can parse the records from a data file:

    (defn read-dct-data
      "Parse lines from `rdr` according to the specification in `dict`.
       Return a lazy sequence of parsed rows."
      [dict rdr]
      (let [parse-fn (make-row-parser dict)]
        (map parse-fn (line-seq rdr))))

Finally, we bring this all together with a function to parse the
dictionary and data and return an Incanter dataset:

    (require '[incanter.core :refer [dataset]])

    (defn as-dataset
      "Read Stata data set, return an Incanter dataset."
      [dict-path data-path]
      (let [dict   (read-dict-defn dict-path)
            header (map (comp keyword :name) dict)]
        (with-open [r (reader data-path)]
          (dataset header (doall (read-dct-data dict r))))))

## Getting the code

The code for all this is available on Github; if you'd like to follow
along, you can fork my
[thinkstats repository](https://github.com/ray1729/thinkstats).

The functions for parsing Stata dictionary files are in the namespace
`thinkstats.dct-parser`.

## Exploring and cleaning the data

Now that we can read the data into an Incanter dataset, we can use
Incanter to explore and clean the data.

    (require '[thinkstats.dct-parser :as dct]
             '[incanter.core :as i :refer [$]])

    (def dict-path "data/2002FemPreg.dct")
    (def data-path "data/2002FemPreg.dat.gz")

    (def ds (dct/as-dataset dict-path data-path))

Incanter's `dim` function tells us the number of rows and columns in a
dataset:

    (i/dim ds)
    ;=> [13593 243]

...and the `col-names` function lists the column names:

    (i/col-names ds)
    ;=> [:caseid :pregordr :howpreg-n :howpreg-p ...]

We can use `sel` to select rows and/or columns from a datatet:
