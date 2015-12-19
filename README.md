# thinkstats

We're working through [Think Stats](http://greenteapress.com/thinkstats2/html/index.html),
attempting the exercises in Clojure.

## Usage

This project includes Allen Downey's ThinkStats2 repository as a git
submodule. To get started:

    git clone git@github.com:ray1729/thinkstats.git --recursive

Note the `--recursive` option to `git clone`; with this option, `git`
will automatically initialize and update the submodule. If you have
cloned without `--recursive`, you'll need to:

    cd thinkstats
    git submodule init
    git submodule update

Read more about git submodules here: https://git-scm.com/book/en/v2/Git-Tools-Submodules

## License

Copyright © 2015 Ray Miller

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
