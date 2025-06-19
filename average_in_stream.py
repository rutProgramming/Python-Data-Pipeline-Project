from collections import defaultdict

# עבור כל תאריך ושעה,  שמרתי את סכום ואת כמות הערכים
# בכל פעם שמגיע ערך נוסף עבור תאריך ושעה ספציפיים מעדכנת את הסכום מוסיפה 1 עבור הכמות הערכים שהגיעו עד כה
# בסוף אני יכולה להחזיר את הממצוע עבור תאריך ושעה ספציפיים ע"ד שמחלקת את הסכום בכמות הערכים
#צריך מחלקה נוספת שמאזינה לאירועים ובכל פעם שמגיע מידע היא שולחת לפונקציה המתאימה
class StreamAverage:
    def __init__(self):
        self.data = defaultdict(lambda: defaultdict(lambda: {'sum': 0, 'count': 0}))
    
    def add_value(self,timestamp,value):
        try:
             date=timestamp.date()
             hour=timestamp.floor('h')
             self.data[date][hour].sum+=value
             self.data[date][hour].count+=1

        except Exception as e:
            return f"Error occurred for {timestamp} {value}: {str(e)}"

        
    def get_average(self, date, hour):
        try:
            total_sum = self.data[date][hour]['sum']
            count = self.data[date][hour]['count']
            return total_sum / count if count > 0 else 0  
        except KeyError:
            return f"No data available for {date} {hour}"

