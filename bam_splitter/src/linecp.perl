#!/usr/bin/perl
## SOURCE: https://unix.stackexchange.com/questions/506207/fast-way-to-extract-lines-from-a-large-file-based-on-line-numbers-stored-in-anot/506226#506226


# usage: thisscript linenumberslist.txt contentsfile

unless (open(IN, $ARGV[0])) {
        die "Can't open list of line numbers file '$ARGV[0]'\n";
}
my %linenumbers = ();
while (<IN>) {
        chomp;
        $linenumbers{$_} = 1;
}

unless (open(IN, "gunzip -c $ARGV[1] |")) {
        die "Can't open contents file '$ARGV[1]'\n";
}
$. = 0;
while (<IN>) {
	print if defined $linenumbers{$.} || $linenumbers{$.-1} || $linenumbers{$.-2} || $linenumbers{$.-3};
}

exit;
