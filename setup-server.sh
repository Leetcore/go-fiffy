if [[ "$OSTYPE" == "linux"* ]]; then
    echo "install requirements"
    sudo apt install python3 python3-pip
    sudo apt update
    sudo apt upgrade -y
fi

pip3 install -r requirements.txt