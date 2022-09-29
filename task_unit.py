# Do Unit testing for the task.py file
#
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
from task import parse, to_csv, get_xml_files

class TestTask(unittest.TestCase):
    def test_parse(self):
        tags = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]
        xml_file = Path("C:/Users/anude/Downloads/tmp/DLTINS_20210117_01of01.xml")
        csv_collector = MagicMock()
        parse(xml_file, tags, csv_collector)
        csv_collector.writerow.assert_called()

    def test_to_csv(self):
        filename = "output.csv"
        to_csv(filename)
        self.assertTrue(Path(f"tmp/{filename}").exists())

if __name__ == "__main__":
    unittest.main()



