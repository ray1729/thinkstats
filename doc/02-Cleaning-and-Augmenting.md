# Cleaning and Augmenting the Data

This is the second instalment of our _Think Stats_ study group; we are
working through Allen Downey's
[Think Stats](http://shop.oreilly.com/product/0636920034094.do),
implementing everything in Clojure. In the
[first part](http://tech.metail.com/think-stats-in-clojure-i/) we
implemented a parser for Stata dictionary and data files. Now we are
going to use that to start exploring the National Survey of Family
Growth data with [Incanter](http://incanter.org/), a Clojure library 
for statistical computing and graphics.

If you'd like to follow along, start by cloning my thinkstats repository from Github:

    git clone git@github.com:ray1729/thinkstats.git --recursive
    
I've made two innovations since writing the first post in this series. I
realised that I could include Allen's repository as a git submodule,
hence the `--recursive` option above. This means the data files will be
in a predictable place in our project folder so we can refer to them in
the examples. I've also included
[Gorilla REPL](http://gorilla-repl.org/) in the project, so if you want to
try out the examples but aren't familiar with the Clojure tool chain,
you can simply run:

    lein gorilla

This will print out a URL for you to open in your browser. You can then
start running the examples and seeing the output in your browser. Read
more about Gorilla REPL here: <http://gorilla-repl.org/>.

## To Business...

Gorilla has created the namespace `harmonious-willow` for our
worksheet. We'll start by importing the Incanter and thinkstats
namespaces we require:

    (ns harmonious-willow
      (:require [incanter.core :as i
                  :refer [$ $map $where $rollup $order $fn $group-by $join]]
                [incanter.stats :as s]
                [thinkstats.dct-parser :as dct]))

Incanter defines a number of handy functions whose names begin with `$`;
we're likely to use these a lot, so I've imported them into our
namespace. We'll refer to the other Incanter functions we need by
qualifying them with the `i/` or `s/` prefix.

Load the NFSG Pregnancy data into an Incanter dataset:

    (def ds (dct/as-dataset "ThinkStats2/code/2002FemPreg.dct"
                            "ThinkStats2/code/2002FemPreg.dat.gz"))

Incanter's `dim` function tells us the number of rows and columns in the dataset:

    (i/dim ds)
    ;=> [13593 243]
    

and `col-names` lists the column names:

    (i/col-names ds)
    ;=> [:caseid :pregordr :howpreg-n :howpreg-p ...]
    
We can select a subset of rows or columns from the dataset using `sel`:

    (i/sel ds :cols [:caseid :pregordr] :rows (range 10))

Either of `:rows` or `:cols` may be omitted, but you'll get a lot of
data back if you ask for all rows. Selecting subsets of the dataset is
such a common thing to do that Incanter provides the function `$` as a
short-cut (but note the different argument order):

    ($ (range 10) [:caseid :pregordr] ds)

If the first argument is omitted, it will return all rows. If you ask
for just a single column and don't wrap the argument in a vector, you
get back a sequence of values for that column:

    (take 10 ($ :caseid ds))
    ;=> ("1" "1" "2" "2" "2" "6" "6" "6" "7" "7")
   
## Cleaning data

Before we start to analyze the data, we may want to remove outliers
or other special values. For example, the `:birthwgt-lb` column gives
the birth weight in pounds of the first baby in the pregnancy. Let's
look at the top 5 values:

    (take 5 (sort > (distinct ($ :birthwgt-lb ds))))
    ;=> Exception thrown: java.lang.NullPointerException
   
Oops! That's not what we wanted, we'll have to remove nil values before
sorting. We can use Incanter's `$where` to do this. Although `$where`
has a number of built-in predicates, there isn't one to check for `nil` values, so we have to write our own:

    (def $not-nil {:$fn (complement nil?)})

    (take 10 ($ :birthwgt-lb ($where {:birthwgt-lb $not-nil} ds)))
    ;=> (8 7 9 7 6 8 9 8 7 6)
    
    (take 5 (sort > (distinct ($ :birthwgt-lb 
                                 ($where {:birthwgt-lb $not-nil} ds)))))
    ;=> (99 98 97 51 15)

This is still a bit cumbersome, so let's write a variant of `sel` that
returns only the rows where none of the specified columns are `nil`:

    (defn ensure-collection
      [x]
      (if (coll? x) x (vector x)))
    
    (defn sel-defined
      [ds & {:keys [rows cols]}]
      (let [rows (or rows :all)
            cols (or cols (i/col-names ds))]
        (i/sel ($where (zipmap (ensure-collection cols) (repeat $not-nil))
                       ds)
               :rows rows :cols cols)))

    (take 5 (sort > (distinct (sel-defined ds :cols :birthwgt-lb))))
    ;=> (99 98 97 51 15)
    
Looking up the definition of `:birthwgt-lb` in the 
[code book](http://www.icpsr.umich.edu/nsfg6/Controller?displayPage=labelDetails&fileCode=PREG&section=&subSec=8014&srtLabel=611802), we see that values greater than 95 encode special meaning:

| Value | Meaning       |
|------:|---------------|
|97     |Not ascertained|
|98     |Refused        |
|99     |Don't know     |

We'd like to remove these values (and the obvious outlier 51) from the
dataset before processing it. Incanter provides the function
`transform-col` that applies a function to each value in the specified
column of a dataset and returns a new dataset. Using this, we can write
a helper function for setting illegal values to `nil`:

    (defn set-invalid-nil
      [ds col valid?]
      (i/transform-col ds col (fn [v] (when (and v (valid? v)) v))))

    (def ds' (set-invalid-nil ds :birthwgt-lb (complement #{51 97 98 99})))

    (take 5 (sort > (distinct (sel-defined ds' :cols :birthwgt-lb))))
    ;=> (15 14 13 12 11)

We should also update the `:birthwgt-oz` column to remove any values
greater than 15:

    (def ds'
        (-> ds
            (set-invalid-nil :birthwgt-lb (complement #{51 97 98 99}))
            (set-invalid-nil :birthwgt-oz (fn [v] (<= 0 v 15)))))

## Transforming data

We used the `transform-col` function in the implementation of
`set-invalid-nil` above. We can also use this to perform an arbitrary
calculation on a value. For example, the `:agepreg` column contains the
age of the participant in centiyears (hundredths of a year):

    (i/head (sel-defined ds' :cols :agepreg))
    ;=> (3316 3925 1433 1783 1833 2700 2883 3016 2808 3233)

Let's transform this to years (perhaps fractional):

    (defn centiyears->years
      [v]
      (when v (/ v 100.0)))

    (def ds' (i/transform-col ds' :agepreg centiyears->years))
    (i/head (sel-defined ds' :cols :agepreg))
    ;=> (33.16 39.25 14.33 17.83 18.33 27.0 28.83 30.16 28.08 32.33)
    
## Augmenting data

The final function we'll show you this time is `add-derived-column`;
this function adds a column to a dataset, where the added column is a
function of other columns. For example:

    (defn compute-totalwgt-lb
      [lb oz]
      (when lb (+ lb (/ (or oz 0) 16.0))))

    (def ds' (i/add-derived-column :totalwgt-lb
                                   [:birthwgt-lb :birthwgt-oz]
                                   compute-totalwgt-lb
                                   ds'))
                                   
    (i/head (sel-defined ds' :cols :totalwgt-lb))
    ;=> (8.8125 7.875 9.125 7.0 6.1875 8.5625 9.5625 8.375 7.5625 6.625)
    
## Putting it all together

We've built up a new dataset above with a number of
transformations. Let's bring these all together into a single function
that will thread the dataset through all these transformations. We can't
use the usual `->` or `->>` macros because of an inconsistency it the
argument order in the transformations, but Clojure's `as->` comes to the
rescue here.

    (defn clean-and-augment-fem-preg
      [ds]
      (as-> ds ds
        (set-invalid-nil ds :birthwgt-lb (complement #{51 97 98 99}))
        (set-invalid-nil ds :birthwgt-oz (fn [v] (<= 0 v 15)))
        (i/transform-col ds :agepreg centiyears->years)
        (i/add-derived-column :totalwgt-lb
                              [:birthwgt-lb :birthwgt-oz]
                              compute-totalwgt-lb
                              ds)))

Now we can do:

    (def ds (clean-and-augment-fem-preg
              (dct/as-dataset "ThinkStats2/code/2002FemPreg.dct"
                              "ThinkStats2/code/2002FemPreg.dat.gz")))

We'll explore this dataset further in the next instalment. The Incanter
helper functions we've implemented can be found in the
`thinkstats.incanter` namespace, along with a `$!` short-cut for
`sel-defined`.
