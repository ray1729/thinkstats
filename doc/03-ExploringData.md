This is the third instalment of our _Think Stats_ study group; we are
working through Allen Downey's
[Think Stats](http://shop.oreilly.com/product/0636920034094.do),
implementing everything in Clojure. In the
[previous part](http://tech.metail.com/think-stats-in-clojure-ii/) we
showed how to use functions from the [Incanter](http://incanter.org/)
library to explore and transform a dataset. Now we build on that
knowledge to explore the National Survey for Family Growth (NFSG) data
and answer the question _do first babies arrive late?_ This takes us
to the end of chapter 1 of the book.

If you'd like to follow along, start by cloning our thinkstats
repository from Github:

    git clone https://github.com/ray1729/thinkstats.git --recursive

Change into the project directory and fire up Gorilla REPL:

    cd thinkstats
    lein gorilla

## Getting Started

Our project includes the namespace `thinkstats.incanter` that brings
together our general Incanter utility functions, and
`thinkstats.family-growth` for the functions we developed last time
for cleaning and augmenting the female pregnancy data.

Let's start by importing these and the Incanter namspaces we're going
to need this time:

    (ns mysterious-aurora
      (:require [incanter.core :as i
                  :refer [$ $map $where $rollup $order $fn $group-by $join]]
                [incanter.stats :as s]
                [thinkstats.gorilla]
                [thinkstats.incanter :as ie :refer [$! $not-nil]]
                [thinkstats.family-growth :as f]))

(We've also included `thinkstats.gorilla`, which just includes some
functionality to render Incanter datasets more nicely in Gorilla REPL.)

The function `thinkstats.family-growth/fem-preg-ds` combines reading
the data set with `clean-and-augment-fem-preg`:

    (def ds (f/fem-preg-ds))

## Validating Data

There are a couple of things covered in chapter 1 of the book that we
haven't done yet: looking af frequencies of values in particular
columns of the NFSG data and validating against the code book, and
building a function to index rows by `:caseid`.

We can use the core Clojure `frequencies` function in conjunction with
Incanter's `$` to select values of a column and return a map of value
to frequency:

    (frequencies ($ :outcome ds))
    ;=> {1 9148, 2 1862, 4 1921, 5 190, 3 120, 6 352}

Incanter's `$rollup` function can be used to compute a summary function
over a column or set of columns, and has built-in support for `:min`,
`:max`, `:mean`, `:sum`, and `:count`. Rolling up `:outcome` by `:count` will
compute the freqency for each outcome and return a new dataset:

    (i/$rollup :count :total :outcome ds)

| :outcome | :total |
|---------:|-------:|
|        1 |   9148 |
|        2 |   1862 |
|        4 |   1921 |
|        5 |    190 |
|        3 |    120 |
|        6 |    352 |    

Compare this with the table in the
[code book](http://www.icpsr.umich.edu/icpsradmin/nsfg/variable/613585?studyNumber=9998&vg=7180).

## Exploring and Interpreting Data

We saw previously that we can use `$where` to select rows matching a
predicate. For example, to select rows for a given `:caseid`:

    ($where {:caseid "10229"} ds)

This could be quite slow for a large dataset as it has to examine
every row. An alternative strategy is to build an index in advance
then use that to select the desired rows. Here's how we might do this:

    (defn build-column-ix
      [col-name ds]
      (reduce (fn [accum [row-ix v]]
                (update accum v (fnil conj []) row-ix))
              {}
              (map-indexed vector ($ col-name ds))))

    (def caseid-ix (build-column-ix :caseid ds))

Now we can quicky select rows for a given `:caseid` using this index:

    (i/sel ds :rows (caseid-ix "10229"))

Recall that we can also select a subset of columns at the same time:

    (i/sel ds :rows (caseid-ix "10229") :cols [:pregordr :agepreg :outcome])

| pregordr | agepreg | outcome |
|---------:|--------:|--------:|
|1         | 19.58   | 4       |
|2         | 21.75   | 4       |
|3         | 23.83   | 4       |
|4         | 25.5    | 4       |
|5         | 29.08   | 4       |
|6         | 32.16   | 4       |
|7         | 33.16   | 1       |

Recall also the meaning of `:outcome`; a value of `4` indicates a
miscarriage and `1` a live birth. So this respondent suffered 6
miscarriages between the ages of 19 and 32, finally seeing a live
birth at age 33.

We can use functions from the `incanter.stats` namespace to compute
basic statistics on our data:

    (s/mean ($! :totalwgt-lb ds))
    (s/median ($! :totalwgt-lb ds))

(Note the use of `$!` to exclude nil values, which would otherwise
trigger a null pointer exception.)

To compute several statistics at once:

    (s/summary ($! [:totalwgt-lb] ds))
    ;=> ({:col :totalwgt-lb, :min 0.0, :max 15.4375, :mean 7.2623018494055485, :median 7.375, :is-numeric true})

Note that, while `mean` and `median` take a sequence of values
(argument to `$!` is just a keyword), the `summary` function expects a
dataset (argument to `$!` is a vector).

## Do First Babies Arrive Late?

We now know enough to have a first attempt at answering this question.
The columns we'll use are:

| Column      | Description                              |
|-------------|------------------------------------------|
| `:outcome`  | Pregnancy outcome (1 == live birth)      |
| `:birthord` | Birth order                              |
| `:prglngth` | Duration of completed pregnancy in weeks |

Compute the mean pregnancy length for the first birth:

    (s/mean ($! :prglngth ($where {:outcome 1 :birthord 1} ds)))
    ;=> 38.60095173351461

...and for subsequent births:

    (s/mean ($! :prglngth ($where {:outcome 1 :birthord {:$ne 1}} ds)))
    ;=> 38.52291446673706

The diffenence between these two values in just 0.08 weeks, so I'd
say that these data do not indicate that first babies arrive late.

Here we've computed mean pregnancy length for first baby and others; if
we want a table of mean pregnancy length by birth order, we can use
`$rollup` again:

    ($rollup :mean :prglngth :birthord (i/$where {:outcome 1 :prglngth $not-nil} ds))

| :birthord |  :prglngth |
|----------:|-----------:|
|         3 | 47501/1234 |
|         4 |  16187/421 |
|         5 |    2419/63 |
|        10 |         36 |
|         9 |       75/2 |
|         7 |     763/20 |
|         1 | 56782/1471 |
|         8 |      263/7 |
|         6 |    1903/50 |
|         2 | 55420/1437 |

The mean has been returned as a rational, but we can use `transform-col`
to convert it to a floating-point number:

    (as-> ds x
          ($where {:outcome 1 :prglngth $not-nil} x)
          ($rollup :mean :prglngth :birthord x)
          (i/transform-col x :prglngth float))

| :birthord | :prglngth |
|----------:|:----------|
|         3 |  38.49352 |
|         4 | 38.448933 |
|         5 | 38.396824 |
|        10 |      36.0 |
|         9 |      37.5 |
|         7 |     38.15 |
|         1 | 38.600952 |
|         8 |  37.57143 |
|         6 |     38.06 |
|         2 |  38.56646 |
        
Finally, we can use `$order` to sort this dataset on birth order:

    (as-> ds x
          ($where {:outcome 1 :prglngth $not-nil} x)
          ($rollup :mean :prglngth :birthord x)
          (i/transform-col x :prglngth float)
          ($order :birthord :asc x))

| :birthord | :prglngth |
|----------:|:----------|
|         1 | 38.600952 |
|         2 |  38.56646 |
|         3 |  38.49352 |
|         4 | 38.448933 |
|         5 | 38.396824 |
|         6 |     38.06 |
|         7 |     38.15 |
|         8 |  37.57143 |
|         9 |      37.5 |
|        10 |      36.0 |

The Incanter functions `$where`, `$rollup`, `$order`, etc. all take a
dataset to act on as their last argument. If this argument is omitted,
they use the dynamic `$data` variable that is usually bound using
`with-data`. So the following two expressions are equivalent:

    ($where {:outcome 1 :prglngth $not-nil} ds)
    
    (with-data ds
      ($where {:outcome 1 :prglngth $not-nil}))
      
It's a bit annoying that we have to use `as->` when we add
`transform-col` to the mix, as this function takes the dataset as its
first argument. Let's add the following to our `thinkstats.incatner`
namespace:

    (defn $transform
      "Like Incanter's `transform-col`, but takes the dataset as an optional
       last argument and, when not specified, uses the dynamically-bound
       `$data`."
      [col f & args]
      (let [[ds args] (if (or (i/matrix? (last args)) (i/dataset? (last args)))
                        [(last args) (butlast args)]
                        [i/$data args])]
        (apply i/transform-col ds col f args)))

Now we can use the `->>` threading macro:

    (->> ($where {:outcome 1 :prglngth $not-nil} ds)
         ($rollup :mean :prglngth :birthord)
         ($transform :prglngth float)
         ($order :birthord :asc))

We have now met most of the core Incanter functions for manipulating
datasets, and a few of the statistics functions.  I hope that, as we get
further into the book, we'll learn how to calculate error bounds for
computed values, and how to decide when we have a statistically
significant result.
