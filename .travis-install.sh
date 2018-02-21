if [ "$TRAVIS_BUILD" = "docs" ]
then
    pip install .
elif [ "$TRAVIS_BUILD" = "tests" ]
then
    psql -U postgres -c "create extension postgis"
    sudo apt-get update
    sudo apt-get install postgresql-9.6-postgis-2.3
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh;
    bash miniconda.sh -b -p $HOME/miniconda
    export PATH="$HOME/miniconda/bin:$PATH"
    hash -r
    conda config --set always_yes yes --set changeps1 no
    conda update -q conda
    conda install pandas
    pip install -r requirements.txt
    git clone --branch development --single-branch https://github.com/meerkat-code/meerkat_api.git
    cd meerkat_api/api_background
    pip install .
    cd ../../
    git clone --branch development --single-branch https://github.com/meerkat-code/meerkat_libs.git
    cd meerkat_libs
    pip install .
    cd ../
fi