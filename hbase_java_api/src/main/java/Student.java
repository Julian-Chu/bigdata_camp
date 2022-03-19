public class Student {
    public String Name;
    private Student.Info Info;
    private Student.Score Score;

    public Student(String name, String studentID, String studentClass, String understanding, String programming) {
        Name = name;
        Info = new Info(studentID, studentClass);
        Score = new Score(understanding, programming);
    }

    class Info{
        public String StudentID;
        public String Class;

        public Info(String studentID, String aClass) {
            StudentID = studentID;
            Class = aClass;
        }
    }

    class Score{
        public String Understanding;
        public String Programming;

        public Score(String understanding, String programming) {
            Understanding = understanding;
            Programming = programming;
        }
    }

    public String getName() {
        return Name;
    }

     public Info getInfo(){
        return this.Info;
     }

     public Score getScore(){
        return this.Score;
     }
}
