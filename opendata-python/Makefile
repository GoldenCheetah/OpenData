venv:
	pipenv install tox tox-pyenv twine

test: venv
	docker-compose -f docker-compose.test.yaml up -d
	- pipenv run tox
	docker-compose down

build: venv
	pipenv run python setup.py sdist bdist_wheel

test_publish: venv build
	pipenv run twine upload --repository-url https://test.pypi.org/legacy/ dist/*

publish: venv build
	pipenv run twine upload dist/*
