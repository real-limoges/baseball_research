for f in *.py
do
    autopep8 --in-place -a $f
done