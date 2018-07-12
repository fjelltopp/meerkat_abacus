if [ "$TRAVIS_BUILD" = "docs" ]
then
    pip install -r ./docs/requirements.txt
    pip install --no-deps .
elif [ "$TRAVIS_BUILD" = "tests" ]
then

    sudo apt-get update
    sudo apt-get install postgresql-9.6-postgis-2.3
    psql -U postgres -c "create extension postgis"
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh;
    bash miniconda.sh -b -p $HOME/miniconda
    export PATH="$HOME/miniconda/bin:$PATH"
    hash -r
    conda config --set always_yes yes --set changeps1 no
    conda update -q conda
    conda install pandas
    pip install -r requirements.txt
    git clone --branch development --single-branch https://github.com/fjelltopp/meerkat_api.git
    cd meerkat_api/api_background
    pip install .
    cd ../../
    git clone --branch development --single-branch https://github.com/fjelltopp/meerkat_libs.git
    cd meerkat_libs
    pip install .
    cd ../
fi
