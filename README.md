<a href="https://github.com/aayush-jain18/AllSpark"><img src="docs/image/allspark.png" height="60px" /></a>
=========================================================================================================================================
AllSpark is a CLI and API ready data comparison tool to get differences between two structured datasets of same or different file format.

AllSpark lets you:

  - Compare structured datasets various formats
  - Compare descriptive statistic differences between datasets
  - Test your ETL flows output
 
 ## Disclaimer

AllSpark is in early stages of development and under constant change, so bugs and issues are expected. We count on your support to find, review and report them!
 
 ## Getting Started

These instructions will get you a copy of the project up and running on your 
local machine for development and testing purposes.

### Prerequisites

Requires Python >= 3.6.2, all dependent python frameworks requirements are 
stated in [requirements.txt](requirements.txt)

  - Clone or download to your desired location
  ```
  git clone https://github.com/aayush-jain18/AllSpark.git
  ```
  - cd to the installation directory AllSpark-master and create a 
   virtualenv to isolate project requirements
  ```
  python -m venv dev
  source dev/bin/activate
  ```
  - Install all the frameworks requirements in your virtualenv
  ```
  pip install -r requirements.txt
  ```
  - Install AllSpark using:
  ```
  pip install -e '.[dev]'
  ```

## Built With

- [Pandas](https://pandas.pydata.org/) - Data structures and Data analysis tools for the Python
- [NumPy](https://www.numpy.org/) - Data structures and Data analysis tools for the Python

## Authors

- **Aayush Jain** - *Author* - 

## License

This project is licensed under the **GNU General Public License v3.0** License - see the 
[LICENSE](LICENSE) file for details.
