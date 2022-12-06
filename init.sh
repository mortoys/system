conda create -n system python=3.8
conda activate system

pip install pip -U
pip config set global.index-url https://mirrors.cloud.tencent.com/pypi/simple/

poetry install
jt -t grade3 -f office
alias nb='jupyter notebook

echo "conda activate q" >> ~/.bashrc
echo "alias nb='jupyter notebook'" >> ~/.bashrc

pip install jupyter_contrib_nbextensions
jupyter contrib nbextension install --user
pip install jupyter_nbextensions_configurator
jupyter nbextensions_configurator enable --user

# export PYTHONPATH=/Users/lumotian/Code/core:$PYTHONPATH
export PYTHONPATH="/Users/lumotian/Code/system/sync:/Users/lumotian/Code/system/layer"
