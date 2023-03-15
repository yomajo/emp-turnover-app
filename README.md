# Employee Turnover Appication

## Watna?

Helping a friend to track employee turnover in select Lihuanian companies. Based on [open government data](https://atvira.sodra.lt/imones/rinkiniai/index.html).

Batteries:
- flask
- sqlite
- pandas

## Screenshots

![screenshot_1](https://user-images.githubusercontent.com/45366313/225350087-3e1bcadc-ff55-4ac8-94fe-d4d1bbadd10f.png)

![screenshot_2](https://user-images.githubusercontent.com/45366313/225350983-a58f919e-1017-4359-bbcb-854091f419cb.png)


## Requirements

- A computer
- [Interniet](https://pbs.twimg.com/media/C7i3LftVMAE_mKj.jpg)
- Python 3.10+
- pipenv
- `.env` file:

`.env` example:
```
FLASK_APP='app.py'
FLASK_DEBUG='true'
ENVIRONMENT='dev'
SECRET_KEY='sure'
```

## Installation Steps

In cmd:

`git clone https://github.com/yomajo/emp-turnover-app.git`

`cd emp-turnover-app`

To localize virtual environment directory:

`mkdir .venv`

`pipenv install`

`pipenv shell`

`flask run`

Visit `127.0.0.1:5000` in browser and take it away from there...