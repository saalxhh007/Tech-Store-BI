import cv2
import pytesseract
from pprint import pprint
import glob

imgs = glob.glob("./../../data/legacy invoices/*.jpg")

def preprocess(roi):
    gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5,5), 0)

    return cv2.adaptiveThreshold(
        gray,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        15,
        3
    )

for img in imgs:
    image = cv2.imread(img)
    if image is None:
        print("Error: Could not load image")
        continue

    info_roi = image[100:250, 30:550]
    products_roi = image[250:400, 30:550]

    info_text = pytesseract.image_to_string(
        preprocess(info_roi),
        config="--psm 6"
    )
    products_text = pytesseract.image_to_string(
        preprocess(products_roi),
        config="--psm 6"
    )
    pprint(info_text)
    pprint(products_text)
    # cv2.rectangle(image, (30,100), (550,250), (255,0,0), 2)
    # cv2.rectangle(image, (30,250), (550,350), (0,255,0), 2)

    # cv2.imshow("ROIs", image)
    # cv2.waitKey(0)
    # cv2.destroyAllWindows()
