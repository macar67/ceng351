package ceng.ceng351.musicdb;

import java.awt.List;

import java.sql.*;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.PreparedStatement;

import ceng.ceng351.musicdb.QueryResult.ArtistNameSongNameGenreRatingResult;
import ceng.ceng351.musicdb.QueryResult.TitleReleaseDateRatingResult;
import ceng.ceng351.musicdb.QueryResult.ArtistNameNumberOfSongsResult;
import ceng.ceng351.musicdb.QueryResult.UserIdUserNameNumberOfSongsResult;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;


public class MUSICDB implements IMUSICDB{
    private static String user = "e2264455";
    private static String password = "84f6bd45";
    private static String host = "144.122.71.57";
    private static String database = "db2264455";
    private static int port = 8084;

    private static Connection con;
    private PreparedStatement p = null;

    @Override
    public void initialize()
    {
        String url = "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.con =  DriverManager.getConnection(url, this.user, this.password);


        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    ///drop tablessss////////
    @Override
    public int dropTables()
    {
        int a,u,s,al,l;
        int numberofTablesDropped = 0;



        String DropAlbumTable = "drop table if exists album ";

        String DropArtistTable = "drop table if exists artist";

        String DropSongTable = "drop table if exists song";


        String DropListenTable = "drop table if exists listen";



        String DropUserTable = "drop table if exists user";

        try {

            Statement statement = this.con.createStatement();
            l = statement.executeUpdate(DropListenTable);
            numberofTablesDropped++;
            u = statement.executeUpdate(DropUserTable);
            numberofTablesDropped++;
            s = statement.executeUpdate(DropSongTable);
            numberofTablesDropped++;
            al = statement.executeUpdate(DropAlbumTable);
            numberofTablesDropped++;
            a = statement.executeUpdate(DropArtistTable);
            numberofTablesDropped++;
            //System.out.println(r);


            //System.out.println(c);


            //System.out.println(a);


            //System.out.println(u);




            //close
            statement.close();

        } catch (SQLException e) {
            // TODO
            e.printStackTrace();
        }


        return numberofTablesDropped;

    }

    @Override
    public int createTables()
    {
        int s , ar, al , l,u;
        int numberofTablesAdded = 0;


        String UserTable ="create table user(userID int not null, username varchar(60), email varchar(30), password varchar(30), primary key (userID))";

        String SongTable = "create table song(songID int not null , songName varchar(60), genre varchar(30), rating double, artistID int, albumID int,primary key (songID),foreign key (albumID) references album(albumID),foreign key (artistID) references artist(artistID) on delete cascade on update cascade)";
        //foreign key (albumID) references Album(albumID),foreign key (artistID) references Artist(artistID) on delete cascade on update cascade

        String ArtistTable ="create table artist(artistID int not null, artistName varchar(60), primary key (artistID))";

        String AlbumTable ="create table album (albumID int not null , title varchar(60), albumGenre varchar(30), albumRating double, releaseDate date, artistID int, primary key (albumId),foreign key (artistID) references artist(artistID) on delete cascade on update cascade)";
        //foreign key (artistID) references Artist(artistID) on delete cascade on update cascade

        String ListenTable="create table listen(userID int not null, songID int not null, lastListenTime timestamp, listenCount int, primary key(userID,songID),foreign key(userID)references user(UserID),foreign key(songID)references song(songID) on delete cascade on update cascade) ";
        //foreign key(userID)references User(UserID)
        //foreign key(songID)references User(songID) on delete cascade on update cascade

        /*user(userID:int, userName:varchar(60), email:varchar(30), password:varchar(30))
        song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        artist(artistID:int, artistName:varchar(60))
        album(albumID:int, title:varchar(60), albumGenre:varchar(30), albumRating:double, releaseDate:date, artistID:int)
        listen(userID:int, songID:int, lastListenTime:timestamp, listenCount:int)*/

        try {


            Statement statement = this.con.createStatement();


            u = statement.executeUpdate(UserTable);
            //System.out.println(u);
            numberofTablesAdded++;

            ar = statement.executeUpdate(ArtistTable);
            //System.out.println(ar);
            numberofTablesAdded++;


            al = statement.executeUpdate(AlbumTable);
            //System.out.println(al);
            numberofTablesAdded++;


            s=statement.executeUpdate(SongTable);
            //System.out.println(s);
            numberofTablesAdded++;


            l = statement.executeUpdate(ListenTable);
            //System.out.println(l);
            numberofTablesAdded++;

            //close
            statement.close();


        } catch (SQLException e) {
            // TODO
            System.out.println("Error has occured on Table Creation");
            e.printStackTrace();
        }
        //System.out.println("create table"+numberofTablesAdded);

        //while(true){


        //}
        return  numberofTablesAdded;
    }


    @Override
    public int insertAlbum(Album[] albums)  {
        int result = 0;
        int counter=0;
        for (int i=0 ; i<albums.length ; i++){
            String title = albums[i].getTitle().replace("'","''");
            String query = "insert into album values ('" +

                    albums[i].getAlbumID()+ "','" +
                    title + "','" +
                    albums[i].getAlbumGenre() + "','" +
                    albums[i].getAlbumRating() + "','" +
                    albums[i].getReleaseDate() + "','" +
                    albums[i].getArtistID() + "')";


            try {
                Statement st = this.con.createStatement();
                result = st.executeUpdate(query);
                 counter ++ ;

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
              //  System.out.println(query);
               /* if (e.toString().contains("SQLIntegrityConstraintViolationException")){
                    throw new AlreadyInsertedException();
                }*/
            }
            result++;
        }

        return counter;
    }

    @Override
    public int insertArtist(Artist[] artists) {
        int result = 0;
        int counter=0;
        for (int i=0 ; i<artists.length ; i++){
            String query = "insert into artist values ('" +

                    artists[i].getArtistID()+ "','" +
                    artists[i].getArtistName() + "')";


            try {
                Statement st = this.con.createStatement();
                result = st.executeUpdate(query);
                counter ++ ;
               // System.out.println(result);
                //System.out.println("ok!!!!");
                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
                /*if (e.toString().contains("SQLIntegrityConstraintViolationException")){
                    throw new AlreadyInsertedException();
                }*/
            }
                    }
        //System.out.println("artist  "+counter);
        return counter;
    }

    @Override
    public int insertSong(Song[] songs) {
        int result =0;
        int counter=0;
        for (int i=0 ; i<songs.length ; i++){
            String newSong=songs[i].getSongName().replace("'","''");
            String query = "insert into song values ('" +

                    songs[i].getSongID()+ "','" +
                    newSong + "','" +
                    songs[i].getGenre() + "','" +
                    songs[i].getRating() + "','" +
                    songs[i].getArtistID() + "','" +
                    songs[i].getAlbumID() + "')";


            try {
                Statement st = this.con.createStatement();
                result = st.executeUpdate(query);
                counter ++ ;

               // System.out.println("ok!!!!");
                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
                /*if (e.toString().contains("SQLIntegrityConstraintViolationException")){
                    throw new AlreadyInsertedException();
                }*/
            }

        }
        return counter;
    }

    @Override
    public int insertUser(User[] users) {
        int result =0;
        int counter=0;
        for (int i=0 ; i<users.length ; i++){
            String query = "insert into user values ('" +

                    users[i].getUserID()+ "','" +
                    users[i].getUserName() + "','" +
                    users[i].getEmail() + "','" +
                    users[i].getPassword() + "')";


            try {
                Statement st = this.con.createStatement();
                result = st.executeUpdate(query);
                counter ++ ;


               // System.out.println(users[i].getUserName());
                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
                /*if (e.toString().contains("SQLIntegrityConstraintViolationException")){
                    throw new AlreadyInsertedException();
                }*/
            }

        }

        return counter;

    }

    @Override
    public int insertListen(Listen[] listens) {
        int result =0;
        int counter=0;
        for (int i=0 ; i<listens.length ; i++){
            String query = "insert into listen values ('" +

                    listens[i].getUserID()+ "','" +
                    listens[i].getSongID() + "','" +
                    listens[i].getLastListenTime() + "','" +
                    listens[i].getListenCount()  +  "')";


            try {
                Statement st = this.con.createStatement();
                result = st.executeUpdate(query);
                counter ++ ;
                //System.out.println("listen ok");

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
                /*if (e.toString().contains("SQLIntegrityConstraintViolationException")){
                    throw new AlreadyInsertedException();
                }*/
            }

        }
        return counter;
    }

    @Override
    public ArtistNameSongNameGenreRatingResult[] getHighestRatedSongs() {
        //ok!
        /*select a.artistName,s.songName,s.genre,s.rating
from song s,artist a
where s.artistID=a.artistID and s.rating= (select max(s2.rating) from song s2);
*/
         ResultSet rs;
        ArrayList< QueryResult.ArtistNameSongNameGenreRatingResult> resultList = new ArrayList< QueryResult.ArtistNameSongNameGenreRatingResult>();
		  String query ="select a.artistName,s.songName,s.genre,s.rating " +
                "from song s,artist a " +
                "where s.artistID=a.artistID and s.rating= (select max(s2.rating) from song s2)+ ';'";
        //System.out.println(query);
		 try {

				Statement st = this.con.createStatement();

				rs = st.executeQuery(query);
				while(rs.next())
				{
                    resultList.add( new QueryResult.ArtistNameSongNameGenreRatingResult(rs.getString("artistName"),rs.getString("songName"),rs.getString("genre"),rs.getInt("rating")));
				}

				st.close();


			} catch (SQLException e) {
				e.printStackTrace();
			}
       // System.out.println("query1=>"+tmp[0]);
        return (QueryResult.ArtistNameSongNameGenreRatingResult[]) resultList.toArray(new QueryResult.ArtistNameSongNameGenreRatingResult[resultList.size()]);

    }

    @Override
    public TitleReleaseDateRatingResult getMostRecentAlbum(String artistName) {

     /* //ok!
        /*select a.title,a.releaseDate,a.albumRating
from album a,artist ar
where ar.artistName='artistName' and ar.artistID=a.albumID ;*/
        ResultSet rs;
        QueryResult.TitleReleaseDateRatingResult[] tmp =new QueryResult.TitleReleaseDateRatingResult[1];
        String query ="select a.title,a.releaseDate,a.albumRating " +
                "from album a,artist ar " +
                "where ar.artistName='"+artistName+"'" + "and ar.artistID=a.albumID + ';'";

        //System.out.println(query);
        try {

            Statement st = this.con.createStatement();

            rs = st.executeQuery(query);
            while(rs.next())
            {
                tmp[0] = new QueryResult.TitleReleaseDateRatingResult(rs.getString("title"),rs.getString("releaseDate"),rs.getInt("albumRating"));
            }

            st.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println("query2=>"+tmp[0]);
        return tmp[0];
        //return (QueryResult.TitleReleaseDateRatingResult[]) resultList.toArray(new QueryResult.TitleReleaseDateRatingResult[resultList.size()]);

    }

    @Override
    public ArtistNameSongNameGenreRatingResult[] getCommonSongs(String userName1, String userName2) {
//ok!
/*select distinct a.artistName,s.songName,s.genre,s.rating
from  song s,user u,listen l,artist a
where u.username="userName1" and a.artistID=s.artistID and u.userID=l.userID and s.songID=l.songID
union
select distinct a.artistName,s.songName,s.genre,s.rating
from  song s,user u,listen l,artist a
where u.username="userName2" and a.artistID=s.artistID and u.userID=l.userID and s.songID=l.songID
order by rating desc; */

        ResultSet rs;
        ArrayList< QueryResult.ArtistNameSongNameGenreRatingResult> resultList = new ArrayList< QueryResult.ArtistNameSongNameGenreRatingResult>();
        String query ="select distinct a.artistName,s.songName,s.genre,s.rating " +
                "from song s,user u,listen l,artist a " +
                "where u.username='"+userName1+"'"+" and a.artistID=s.artistID and u.userID=l.userID and s.songID=l.songID " +
                "union "+
                "select distinct a.artistName,s.songName,s.genre,s.rating "+
                "from  song s,user u,listen l,artist a "+
                "where u.username='"+userName2+"'"+" and a.artistID=s.artistID and u.userID=l.userID and s.songID=l.songID "+
                "order by rating desc "+ ';';


       // System.out.println(query);
        try {

            Statement st = this.con.createStatement();

            rs = st.executeQuery(query);
            while(rs.next())
            {
                resultList.add( new ArtistNameSongNameGenreRatingResult(rs.getString("artistName"),rs.getString("songName"),rs.getString("genre"),rs.getDouble("rating")));
            }

            st.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println("query3=>"+tmp[0]);


        return (QueryResult.ArtistNameSongNameGenreRatingResult[]) resultList.toArray(new QueryResult.ArtistNameSongNameGenreRatingResult[resultList.size()]);
    }

    @Override
    public ArtistNameNumberOfSongsResult[] getNumberOfTimesSongsListenedByUser(String userName) {
        //ok
        /*select distinct a.artistName,sum(l.listenCount)
                from song s,user u,listen l,artist a
                where u.username ="Alperen Dalkiran"
				and u.userID=l.userID and l.songID=s.songID and s.artistID=a.artistID
                group by a.artistName ;
*/
        ResultSet rs;
        ArrayList< QueryResult.ArtistNameNumberOfSongsResult> resultList = new ArrayList< QueryResult.ArtistNameNumberOfSongsResult>();
        String query ="select distinct a.artistName,sum(l.listenCount) as numberofSong " +
                "from song s,user u,listen l,artist a " +
                "where u.username ='"+userName+"'"+
                " and u.userID=l.userID and l.songID=s.songID and s.artistID=a.artistID " +
                "group by a.artistName " + ';';

       // System.out.println(query);
        try {

            Statement st = this.con.createStatement();

            rs = st.executeQuery(query);
            while(rs.next())
            {
                resultList.add( new QueryResult.ArtistNameNumberOfSongsResult(rs.getString("artistName"),rs.getInt("numberofSong")));
            }

            st.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }
        //System.out.println("query4=>"+tmp[0]);

        return (QueryResult.ArtistNameNumberOfSongsResult[]) resultList.toArray(new QueryResult.ArtistNameNumberOfSongsResult[resultList.size()]);
    }

    @Override
    public User[] getUsersWhoListenedAllSongs(String artistName) {
/*select *
from user u
where not exists (
select *
from song s,artist a
where a.artistName='ADELE' and a.artistID=s.artistID and s.songID not in (select l.songID
																		from listen l
                                                                        where u.userID=l.userID));

*/
        ResultSet rs;
        ArrayList<User> resultList = new ArrayList<User>();

        String query ="select * " +
                "from user u " +
                "where not exists ( " +
                " select * " +
                " from song s,artist a " +
                " where a.artistName= '"+artistName+"'" +
                " and a.artistID=s.artistID "+
                " and s.songID not in (select l.songID "+
                " from listen l "+
                " where u.userID=l.userID)) " +  ';';
        //System.out.println(query);
       try
        {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);
            while(rs.next())
            {
                resultList.add(new User(rs.getInt("userID"), rs.getString("userName"),rs.getString("email"),rs.getString("password")));
            }

            st.close();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
         //System.out.println("result list "+resultList);
        return (User []) resultList.toArray(new User[resultList.size()]);



    }

    @Override
    public UserIdUserNameNumberOfSongsResult[] getUserIDUserNameNumberOfSongsNotListenedByAnyone() {
    /*select distinct u.userID,u.username,count(*) as numberOfSongs
from user u,song s,listen l
where l.userID=u.userID  and s.songID=l.songID and
s.songID not in (
select distinct s2.songID
from user u2,song s2,listen l2
where l2.userID=u2.userID and u2.userID!=u.userID  and s2.songID=l2.songID)
Group by u.userID
ORDER BY u.userID;*/
        ResultSet rs;
       // QueryResult.UserIdUserNameNumberOfSongsResult[] tmp =new QueryResult.UserIdUserNameNumberOfSongsResult[2];
        ArrayList< QueryResult.UserIdUserNameNumberOfSongsResult> resultList = new ArrayList< QueryResult.UserIdUserNameNumberOfSongsResult>();
       String query ="select distinct u.userID,u.username,count(*) as numberOfSongs " +
                "from user u,song s,listen l  " +
                "where l.userID=u.userID  and s.songID=l.songID " +
                "and s.songID not in ( " +
                " select distinct s2.songID " +
                " from user u2,song s2,listen l2 " +
                "where l2.userID=u2.userID " +
                "and u2.userID!=u.userID " +
                "and s2.songID=l2.songID ) " +
                "group by u.userID "+
                "order by u.userID " + ';';


        try {

            Statement st = this.con.createStatement();

            rs = st.executeQuery(query);
           // int i = 0 ;
            while(rs.next())
            {
                resultList.add(new QueryResult.UserIdUserNameNumberOfSongsResult(rs.getInt("userID"),rs.getString("username"),rs.getInt("numberOfSongs")));
                //i++;
            }

            st.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }

        //System.out.println("evet bekliyoruz.."+resultList);
        return (UserIdUserNameNumberOfSongsResult []) resultList.toArray(new UserIdUserNameNumberOfSongsResult[resultList.size()]);

    }

    @Override
    public Artist[] getArtistSingingPopGreaterAverageRating(double rating) {
//ok
/*select  a.artistID,a.artistName
from artist a,song s
where s.artistID=a.artistID and s.genre="pop"
group by a.artistID
having avg(rating)>rating;
*/
        ResultSet rs;
        ArrayList<Artist> resultList = new ArrayList<Artist>();

        String query ="select  a.artistID,a.artistName " +
                "from artist a,song s  " +
                "where s.artistID=a.artistID " +
                "and s.genre='pop' " +
                " group by a.artistID" +
                " having avg(rating) > "+rating+""+"   " + ';';
try
		{
			Statement st = this.con.createStatement();
			rs = st.executeQuery(query);
			while(rs.next())
			{
				resultList.add(new Artist(rs.getInt("artistID"), rs.getString("artistName")));
			}

			st.close();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
       // System.out.println("result list "+resultList);
		return (Artist []) resultList.toArray(new Artist[resultList.size()]);

    }

    @Override
    public Song[] retrieveLowestRatedAndLeastNumberOfListenedSong() {
/*select *
        from ( select temp.songID,(sum(l.listenCount)) as abc
                from listen l ,( select * from song s where s.rating=(select min(s2.rating)
                from song s2  where s2.genre='pop' )) as temp
        where l.songID=temp.songID group by temp.songID  order by abc ) as temp2,song s3
        where temp2.songID=s3.songID ;

*/
        ResultSet rs;
        ArrayList<Song> resultList = new ArrayList<Song>();

        String query =" select s3.songID,s3.songName,s3.rating,s3.genre,s3.artistID,s3.albumID  " +
                "from ( "+"select temp.songID,(sum(l.listenCount)) as abc  " +
                "from listen l ," + "( select * " +"from song s"+" where s.rating=(select min(s2.rating) "+
                "from song s2 " +
                " where s2.genre='pop' )) as temp " +
                " where l.songID=temp.songID "+
                "group by temp.songID "+
                " order by abc ) as temp2,song s3 "  +
                "  where temp2.songID=s3.songID ;"+  ';';
       // System.out.println(query);
        try
        {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

           if(rs.next())
            {
                resultList.add(new Song(rs.getInt("songID"),rs.getString("songName"),rs.getString("genre"),rs.getDouble("rating"),rs.getInt("artistID"),rs.getInt("albumID")));

            }

            st.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }



        //System.out.println(resultList);
       //sadece  listenin ilk elemanını dönmeli
       //System.out.println();
        return (Song []) resultList.toArray(new Song[resultList.size()]);
    }

    @Override
    public int multiplyRatingOfAlbum(String releaseDate) {
//ok!
/*update album  set album.albumRating=album.albumRating*(1.5) where album.releaseDate>releaseDate;*/
    ResultSet rs;
    //int result = 0;
    int count = 0;
    String query ="update album " +
            "set album.albumRating=album.albumRating*(1.5)"+
            "where album.releaseDate > '" + releaseDate + "';";


    try {
        Statement st = this.con.createStatement();
         st.executeUpdate(query);
        count++;


        //Close
        st.close();
    } catch (SQLException e) {
        e.printStackTrace();
    }



   return count;
    }
    @Override
    public Song deleteSong(String songName) {
       //ok!
      ResultSet rs;

        String query2 ="select s.songID,s.songName,s.rating,s.genre,s.artistID,s.albumID " +
                "from song s " +
                "where s.songName='" + songName +"'" +';';


        String query="delete " +
                "from song  " +
                "where song.songName='" + songName +"'" + ';';
       // System.out.println(query);


        int sid=0 ;
        String sname="";
        double srating=0;
        String sgenre="";
        int aid=0;
        int alid=0;

        try {
            Statement st = this.con.createStatement();

            rs=st.executeQuery(query2);
            rs.next();

            sid=rs.getInt(1);
            sname=rs.getString(2);
            srating=rs.getDouble(3);
            sgenre=rs.getString(4);
            aid=rs.getInt(5);
            alid=rs.getInt(6);

              st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {

            Statement st2 = this.con.createStatement();
            st2.executeUpdate(query);
            st2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Song my_song = new Song(sid,sname,sgenre,srating,aid,alid);
        my_song.setSongID(sid);
        my_song.setSongName(sname);
        my_song.setRating(srating);
        my_song.setGenre(sgenre);
        my_song.setArtistID(aid);
        my_song.setAlbumID(alid);
        //System.out.println(my_song);
        return my_song ;

    }


/*delete from song s where s.songName=this.songName*/




}
