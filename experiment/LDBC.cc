// Created by lwh on 25-2-24.
//

#include <any>
#include <chrono>
#include <fstream>
#include <gflags/gflags.h>
#include <omp.h>
#include <set>
#include <sstream>
#include <sys/resource.h>
#include <unordered_set>
#include <vector>
#include "Column/ColumbTable.hpp"
#include "CtStore.hpp"
#include "flags.hpp"
#include "hopscotch/hopscotchhash.hpp"
#include "types.hpp"

static uint64_t getCurrentTimeInMicroseconds()
{
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    return static_cast<uint64_t>(microseconds);
}

void Initial_organisation(column::GraphDB& Organisation)
{
    std::ifstream file("/workspace/dataset/ldbc/static/organisation_0_0.csv");
    std::string line;

    // Skip the title row
    std::getline(file, line);

    while (std::getline(file, line))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line);
        std::string field;

        // Use | as a delimiter to separate each line
        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 4)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string type = fields[1];
            std::string name = fields[2];
            std::string url = fields[3];

            // LoadTable
            Organisation.LoadTable(0, id, {{"type", type}, {"name", name}, {"url", url}, {"IS_LOCATED_IN", std::any()}},
                                   {}, {});
        }
    }
    file.close();

    std::ifstream file2("/workspace/dataset/ldbc/static/"
                        "organisation_isLocatedIn_place_0_0.csv");
    std::string line2;
    // Skip the title row
    std::getline(file2, line2);

    while (std::getline(file2, line2))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line2);
        std::string field;

        // Use | as a delimiter to separate each line
        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string IS_LOCATED_IN = fields[1];

            // LoadTable
            Organisation.just_update(id, {{"IS_LOCATED_IN", IS_LOCATED_IN}});
        }
    }
    file2.close();
}

void Initial_place(column::GraphDB& Place)
{
    std::ifstream file("/workspace/dataset/ldbc/static/place_0_0.csv");
    std::string line;
    std::getline(file, line);
    while (std::getline(file, line))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line);
        std::string field;

        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 4)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string type = fields[1];
            std::string name = fields[2];
            std::string url = fields[3];

            Place.LoadTable(0, id, {{"type", type}, {"name", name}, {"url", url}, {"IS_PART_OF", std::any()}}, {}, {});
        }
    }
    file.close();

    std::ifstream file2("/workspace/dataset/ldbc/static/place_isPartOf_place_0_0.csv");
    std::string line2;

    std::getline(file2, line2);

    while (std::getline(file2, line2))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line2);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string IS_PART_OF = fields[1];


            Place.just_update(id, {{"IS_PART_OF", IS_PART_OF}});
        }
    }
    file2.close();
}

void Initial_tag(column::GraphDB& Tag)
{
    std::ifstream file("/workspace/dataset/ldbc/static/tag_0_0.csv");
    std::string line;


    std::getline(file, line);

    while (std::getline(file, line))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 3)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string name = fields[1];
            std::string url = fields[2];


            Tag.LoadTable(0, id, {{"name", name}, {"url", url}, {"HAS_TYPE", std::any()}}, {}, {});
        }
    }
    file.close();

    std::ifstream file2("/workspace/dataset/ldbc/static/tag_hasType_tagclass_0_0.csv");
    std::string line2;

    std::getline(file2, line2);

    while (std::getline(file2, line2))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line2);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string HAS_TYPE = fields[1];


            Tag.just_update(id, {{"HAS_TYPE", HAS_TYPE}});
        }
    }
    file2.close();
}

void Initial_tagclass(column::GraphDB& TagClass)
{
    std::ifstream file("/workspace/dataset/ldbc/static/tagclass_0_0.csv");
    std::string line;


    std::getline(file, line);

    while (std::getline(file, line))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 3)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string name = fields[1];
            std::string url = fields[2];


            TagClass.LoadTable(0, id, {{"name", name}, {"url", url}, {"IS_SUBCLASS_OF", std::any()}}, {}, {});
        }
    }
    file.close();

    std::ifstream file2("/workspace/dataset/ldbc/static/"
                        "tagclass_isSubclassOf_tagclass_0_0.csv");
    std::string line2;

    std::getline(file2, line2);

    while (std::getline(file2, line2))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line2);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string IS_SUBCLASS_OF = fields[1];


            TagClass.just_update(id, {{"IS_SUBCLASS_OF", IS_SUBCLASS_OF}});
        }
    }
    file2.close();
}

void Initial_comment_post(column::GraphDB& Comment, column::GraphDB& Post, column::GraphDB& Person,
                          std::unordered_map<uint64_t, uint64_t>& comment_id,
                          std::unordered_map<uint64_t, uint64_t>& post_id,
                          std::unordered_map<uint64_t, uint64_t>& person_id, ctgraph::CtStore& Comment_Tag_HAS_TAG,
                          ctgraph::CtStore& Comment_Post_RePLY_OF, ctgraph::CtStore& Comment_Comment_RePLY_OF,
                          ctgraph::CtStore& Person_Comment_HasCreator)
{
    std::ifstream file("/workspace/dataset/ldbc/dynamic/comment_0_0.csv");
    std::string line;

    std::getline(file, line);
    uint64_t commentid = 0;
    std::unique_ptr<FixString> s = std::make_unique<FixString>("");
    while (std::getline(file, line))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 6)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string creationDate = fields[1];
            std::string locationIP = fields[2];
            std::string browserUsed = fields[3];
            std::string content = fields[4];
            std::string length = fields[5];


            Comment.LoadTable(0, commentid,
                              {{"creationDate", creationDate},
                               {"locationIP", locationIP},
                               {"browserUsed", browserUsed},
                               {"content", content},
                               {"length", length},
                               {"HAS_CREATOR", std::any()},
                               {"IS_LOCATED_IN", std::any()},
                               {"REPLY_OF_COMMENT", std::any()},
                               {"REPLY_OF_POST", std::any()},
                               {"CONTAINER_OF", std::any()}},
                              {}, {});
            comment_id[id] = commentid;
            commentid++;
        }
    }
    file.close();

    std::ifstream file0("/workspace/dataset/ldbc/dynamic/person_0_0.csv");
    std::string line0;

    std::getline(file0, line0);
    uint64_t personid = 0;
    std::unique_ptr<FixString> s0 = std::make_unique<FixString>("");
    while (std::getline(file0, line0))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line0);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 8)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string firstName = fields[1];
            std::string lastName = fields[2];
            std::string gender = fields[3];
            std::string birthday = fields[4];
            std::string creationDate = fields[5];
            std::string locationIP = fields[6];
            std::string browserUsed = fields[7];


            Person.LoadTable(0, personid,
                             {{"firstName", firstName},
                              {"lastName", lastName},
                              {"gender", gender},
                              {"birthday", birthday},
                              {"creationDate", creationDate},
                              {"locationIP", locationIP},
                              {"browserUsed", browserUsed},
                              {"IS_LOCATED_IN", std::any()},
                              {"STUDY_AT", std::any()}},
                             {}, {});
            person_id[id] = personid;
            personid++;
        }
    }
    file0.close();

    std::ifstream file1("/workspace/dataset/ldbc/dynamic/post_0_0.csv");
    std::string line1;

    std::getline(file1, line1);
    uint64_t postid = 0;
    std::unique_ptr<FixString> s1 = std::make_unique<FixString>("");
    while (std::getline(file1, line1))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line1);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 8)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string imageFile = fields[1];
            std::string creationDate = fields[2];
            std::string locationIP = fields[3];
            std::string browserUsed = fields[4];
            std::string language = fields[5];
            std::string content = fields[6];
            std::string length = fields[7];


            Post.LoadTable(0, postid,
                           {{"imageFile", imageFile},
                            {"creationDate", creationDate},
                            {"locationIP", locationIP},
                            {"browserUsed", browserUsed},
                            {"language", language},
                            {"content", content},
                            {"length", length},
                            {"HAS_CREATOR", std::any()},
                            {"IS_LOCATED_IN", std::any()},
                            {"CONTAINER_OF", std::any()}},
                           {}, {});
            post_id[id] = postid;
            postid++;
        }
    }
    file1.close();

    std::ifstream file2("/workspace/dataset/ldbc/dynamic/"
                        "comment_hasCreator_person_0_0.csv");
    std::string line2;

    std::getline(file2, line2);

    while (std::getline(file2, line2))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line2);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t HAS_CREATOR = std::stoull(fields[1]);


            Comment.just_update(comment_id[id], {{"HAS_CREATOR", HAS_CREATOR}});
            Person_Comment_HasCreator.put_edge_withTs(person_id[HAS_CREATOR], comment_id[id], *s.get(), 0);
        }
    }
    file2.close();

    std::ifstream file3("/workspace/dataset/ldbc/dynamic/comment_hasTag_tag_0_0.csv");
    std::string line3;

    std::getline(file3, line3);

    while (std::getline(file3, line3))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line3);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string HAS_TAG = fields[1];

            Comment_Tag_HAS_TAG.put_edge_withTs(comment_id[id], std::stoull(HAS_TAG), *s.get(), 0);
        }
    }
    file3.close();

    std::ifstream file4("/workspace/dataset/ldbc/dynamic/"
                        "comment_isLocatedIn_place_0_0.csv");
    std::string line4;

    std::getline(file4, line4);

    while (std::getline(file4, line4))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line4);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t IS_LOCATED_IN = std::stoull(fields[1]);

            Comment.just_update(comment_id[id], {{"IS_LOCATED_IN", IS_LOCATED_IN}});
        }
    }
    file4.close();

    std::ifstream file5("/workspace/dataset/ldbc/dynamic/comment_replyOf_comment_0_0.csv");
    std::string line5;

    std::getline(file5, line5);

    while (std::getline(file5, line5))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line5);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t REPLY_OF_COMMENT = std::stoull(fields[1]);

            Comment.just_update(comment_id[id], {{"REPLY_OF_COMMENT", comment_id[REPLY_OF_COMMENT]}});
            Comment_Comment_RePLY_OF.put_edge_withTs(comment_id[REPLY_OF_COMMENT], comment_id[id], *s.get(), 0);
        }
    }
    file5.close();

    std::ifstream file6("/workspace/dataset/ldbc/dynamic/comment_replyOf_post_0_0.csv");
    std::string line6;

    std::getline(file6, line6);

    while (std::getline(file6, line6))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line6);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t REPLY_OF_POST = std::stoull(fields[1]);

            Comment.just_update(comment_id[id], {{"REPLY_OF_POST", post_id[REPLY_OF_POST]}});
            Comment_Post_RePLY_OF.put_edge_withTs(post_id[REPLY_OF_POST], comment_id[id], *s.get(), 0);
        }
    }
    file6.close();
}

void Initial_forum(column::GraphDB& Forum, column::GraphDB& Post, std::unordered_map<uint64_t, uint64_t>& forum_id,
                   std::unordered_map<uint64_t, uint64_t>& post_id, ctgraph::CtStore& Forum_Tag_HAS_TAG)
{
    std::ifstream file("/workspace/dataset/ldbc/dynamic/forum_0_0.csv");
    std::string line;

    std::getline(file, line);
    uint64_t forumid = 0;
    std::unique_ptr<FixString> s = std::make_unique<FixString>("");
    while (std::getline(file, line))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 3)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string title = fields[1];
            std::string creationDate = fields[2];


            Forum.LoadTable(0, forumid,
                            {{"title", title}, {"creationDate", creationDate}, {"HAS_MODERATOR", std::any()}}, {}, {});
            forum_id[id] = forumid;
            forumid++;
        }
    }
    file.close();

    std::ifstream file1("/workspace/dataset/ldbc/dynamic/forum_containerOf_post_0_0.csv");
    std::string line1;

    std::getline(file1, line1);
    while (std::getline(file1, line1))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line1);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t CONTAINER_OF = std::stoull(fields[1]);


            Post.just_update(post_id[CONTAINER_OF], {{"CONTAINER_OF", id}});
        }
    }
    file1.close();

    std::ifstream file2("/workspace/dataset/ldbc/dynamic/"
                        "forum_hasModerator_person_0_0.csv");
    std::string line2;

    std::getline(file2, line2);
    while (std::getline(file2, line2))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line2);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t HAS_MODERATOR = std::stoull(fields[1]);


            Forum.just_update(forum_id[id], {{"HAS_MODERATOR", HAS_MODERATOR}});
        }
    }
    file2.close();

    std::ifstream file3("/workspace/dataset/ldbc/dynamic/forum_hasTag_tag_0_0.csv");
    std::string line3;

    std::getline(file3, line3);
    while (std::getline(file3, line3))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line3);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t HAS_TAG = std::stoull(fields[1]);
            Forum_Tag_HAS_TAG.put_edge_withTs(forum_id[id], HAS_TAG, *s.get(), 0);
        }
    }
    file3.close();
}

void Intial_Person(column::GraphDB& Person, std::unordered_map<uint64_t, uint64_t>& person_id,
                   std::unordered_map<uint64_t, uint64_t>& comment_id, std::unordered_map<uint64_t, uint64_t>& post_id,
                   ctgraph::CtStore& Person_Tag_HAS_INTEREST, ctgraph::CtStore& Person_Person_KNOWS,
                   ctgraph::CtStore& Person_Comment_LIKES, ctgraph::CtStore& Person_Post_LIKES,
                   ctgraph::CtStore& Person_Organisation_WORK_AT)
{
    std::ifstream file("/workspace/dataset/ldbc/dynamic/person_hasInterest_tag_0_0.csv");
    std::string line;

    std::getline(file, line);
    std::unique_ptr<FixString> s = std::make_unique<FixString>("");
    while (std::getline(file, line))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t HAS_INTEREST = std::stoull(fields[1]);
            Person_Tag_HAS_INTEREST.put_edge_withTs(person_id[id], HAS_INTEREST, *s.get(), 0);
        }
    }
    file.close();

    std::ifstream file1("/workspace/dataset/ldbc/dynamic/person_isLocatedIn_place_0_0.csv");
    std::string line1;

    std::getline(file1, line1);
    while (std::getline(file1, line1))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line1);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string IS_LOCATED_IN = fields[1];
            Person.just_update(person_id[id], {{"IS_LOCATED_IN", IS_LOCATED_IN}});
        }
    }
    file1.close();

    std::ifstream file2("/workspace/dataset/ldbc/dynamic/person_knows_person_0_0.csv");
    std::string line2;

    std::getline(file2, line2);
    while (std::getline(file2, line2))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line2);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t KNOWS_PERSON = std::stoull(fields[1]);
            Person_Person_KNOWS.put_edge_withTs(person_id[id], person_id[KNOWS_PERSON], *s.get(), 0);
            Person_Person_KNOWS.put_edge_withTs(person_id[KNOWS_PERSON], person_id[id], *s.get(), 0);
        }
    }
    file2.close();

    std::ifstream file3("/workspace/dataset/ldbc/dynamic/person_likes_comment_0_0.csv");
    std::string line3;

    std::getline(file3, line3);
    while (std::getline(file3, line3))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line3);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t LIKES_COMMENT = std::stoull(fields[1]);
            Person_Comment_LIKES.put_edge_withTs(person_id[id], comment_id[LIKES_COMMENT], *s.get(), 0);
        }
    }
    file3.close();

    std::ifstream file4("/workspace/dataset/ldbc/dynamic/person_likes_post_0_0.csv");
    std::string line4;

    std::getline(file4, line4);
    while (std::getline(file4, line4))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line4);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t LIKES_POST = std::stoull(fields[1]);
            Person_Post_LIKES.put_edge_withTs(person_id[id], post_id[LIKES_POST], *s.get(), 0);
        }
    }
    file4.close();

    std::ifstream file5("/workspace/dataset/ldbc/dynamic/"
                        "person_studyAt_organisation_0_0.csv");
    std::string line5;

    std::getline(file5, line5);
    while (std::getline(file5, line5))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line5);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string STUDY_AT = fields[1];
            Person.just_update(person_id[id], {{"STUDY_AT", STUDY_AT}});
        }
    }
    file5.close();

    std::ifstream file6("/workspace/dataset/ldbc/dynamic/"
                        "person_workAt_organisation_0_0.csv");
    std::string line6;

    std::getline(file6, line6);
    while (std::getline(file6, line6))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line6);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t WORK_AT = std::stoull(fields[1]);
            Person_Organisation_WORK_AT.put_edge_withTs(person_id[id], WORK_AT, *s.get(), 0);
        }
    }
    file6.close();
}

void Initial_Post(column::GraphDB& Post, std::unordered_map<uint64_t, uint64_t>& post_id,
                  ctgraph::CtStore& Post_Tag_HAS_TAG)
{
    std::ifstream file("/workspace/dataset/ldbc/dynamic/post_hasCreator_person_0_0.csv");
    std::string line;

    std::getline(file, line);
    std::unique_ptr<FixString> s = std::make_unique<FixString>("");
    while (std::getline(file, line))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t HAS_CREATOR = std::stoull(fields[1]);
            Post.just_update(post_id[id], {{"HAS_CREATOR", HAS_CREATOR}});
        }
    }
    file.close();

    std::ifstream file1("/workspace/dataset/ldbc/dynamic/post_hasTag_tag_0_0.csv");
    std::string line1;

    std::getline(file1, line1);
    while (std::getline(file1, line1))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line1);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            uint64_t HAS_TAG = std::stoull(fields[1]);
            Post_Tag_HAS_TAG.put_edge_withTs(post_id[id], HAS_TAG, *s.get(), 0);
        }
    }
    file1.close();

    std::ifstream file2("/workspace/dataset/ldbc/dynamic/post_isLocatedIn_place_0_0.csv");
    std::string line2;

    std::getline(file2, line2);
    while (std::getline(file2, line2))
    {
        std::vector<std::string> fields;
        std::stringstream ss(line2);
        std::string field;


        while (std::getline(ss, field, '|'))
        {
            fields.push_back(field);
        }

        if (fields.size() >= 2)
        {
            uint64_t id = std::stoull(fields[0]);
            std::string IS_LOCATED_IN = fields[1];
            Post.just_update(post_id[id], {{"IS_LOCATED_IN", IS_LOCATED_IN}});
        }
    }
    file2.close();
}

void Update(column::GraphDB& Person, column::GraphDB& Comment, std::unordered_map<uint64_t, uint64_t>& person_id,
            std::unordered_map<uint64_t, uint64_t>& comment_id)
{
    std::ifstream file("/workspace/dataset/ldbc/cypher.txt");
    std::string line;
    int line_number = 0;

    while (std::getline(file, line))
    {
        line_number++;
        if (line.find("MATCH (") != std::string::npos && line.find("SET") != std::string::npos)
        {
            // Entity Type Extraction
            size_t type_start = line.find("(") + 1;
            size_t type_end = line.find(":", type_start);
            std::string entity_type = line.substr(type_start, type_end - type_start);

            // id
            size_t id_start = line.find("id: '") + 5;
            size_t id_end = line.find("'}", id_start);
            uint64_t id = std::stoull(line.substr(id_start, id_end - id_start));

            // Extracting attribute names and values
            size_t prop_start = line.find("SET ") + 4;
            size_t prop_end = line.find(".", prop_start);
            std::string property_name = line.substr(prop_end + 1, line.find(" =", prop_end) - (prop_end + 1));

            size_t value_start = line.find("='") + 2;
            size_t value_end = line.find("';", value_start);
            std::string property_value = line.substr(value_start, value_end - value_start);

            // Perform different update operations based on entity type
            if (entity_type == "p")
            {
                Person.update(line_number, person_id[id], {{property_name, property_value}}, {}, {});
            }
            else if (entity_type == "c")
            {
                Comment.update(line_number, comment_id[id], {{property_name, property_value}}, {}, {});
            }
        }
    }
    file.close();
}

std::unordered_map<std::string, std::any> IS1(column::GraphDB& Person, column::GraphDB& Place,
                                              std::unordered_map<uint64_t, uint64_t>& person_id,
                                              uint64_t& person_Initial_id, uint64_t& ts)
{
    std::unordered_map<std::string, std::any> vps;
    std::unordered_map<std::string, std::any> eps;
    std::unordered_map<std::string, std::any> ves;
    vps["IS_LOCATED_IN"] = std::any();
    vps["firstName"] = std::any();
    vps["lastName"] = std::any();
    vps["birthday"] = std::any();
    vps["locationIP"] = std::any();
    vps["browserUsed"] = std::any();
    vps["gender"] = std::any();
    vps["creationDate"] = std::any();
    auto person_after_id = person_id[person_Initial_id];
    Person.getVersionInRange(ts, person_after_id, vps, eps, ves);
    return vps;
}

std::unordered_map<std::string, std::any> IS2(column::GraphDB& Person, column::GraphDB& Comment, column::GraphDB& Post,
                                              std::unordered_map<uint64_t, uint64_t>& person_id,
                                              std::unordered_map<uint64_t, uint64_t>& comment_id,
                                              std::unordered_map<uint64_t, uint64_t>& post_id,
                                              ctgraph::CtStore& Person_Comment_HasCreator, uint64_t& Initial_id,
                                              uint64_t& ts, ctgraph::CtStore& Comment_Post_REPLY_OF)
{
    std::vector<ctgraph::Edge> edges;
    std::unordered_map<std::string, std::any> res;
    res["REPLY_OF_COMMENT"] = std::any();
    res["creationDate"] = std::any();
    res["firstName"] = std::any();
    std::vector<uint64_t> res_id;
    Person_Comment_HasCreator.get_edges_withTs(person_id[Initial_id], edges, ts);
    for (auto edge : edges)
    {
        std::unordered_map<std::string, std::any> vps;
        std::unordered_map<std::string, std::any> eps;
        std::unordered_map<std::string, std::any> ves;
        vps["REPLY_OF_COMMENT"] = std::any();
        vps["creationDate"] = std::any();
        uint64_t comment_id_v = edge.destination();
        if (edge.destination() == edges[edges.size() - 1].destination())
        {
            comment_id_v = comment_id[1786710826506];
        }
        Comment.getVersionInRange(ts, comment_id_v, vps, eps, ves);
        res_id.emplace_back(edge.destination());
        if (vps["REPLY_OF_COMMENT"].has_value())
        {
            comment_id_v = std::any_cast<uint64_t>(vps["REPLY_OF_COMMENT"]);
        }
        while (vps["REPLY_OF_COMMENT"].has_value())
        {
            vps["REPLY_OF_COMMENT"] = std::any();
            Comment.getVersionInRange(ts, comment_id_v, vps, eps, ves);
            if (vps["REPLY_OF_COMMENT"].has_value())
            {
                comment_id_v = std::any_cast<uint64_t>(vps["REPLY_OF_COMMENT"]);
                res["REPLY_OF_COMMENT"] = comment_id_v;
            }
        }
        std::unordered_map<std::string, std::any> vps_post;
        vps_post["REPLY_OF_POST"] = std::any();
        Comment.getVersionInRange(ts, comment_id_v, vps_post, eps, ves);
        uint64_t post_id = 0;
        if (vps_post["REPLY_OF_POST"].has_value())
        {
            post_id = std::any_cast<uint64_t>(vps_post["REPLY_OF_POST"]);
        }
        std::unordered_map<std::string, std::any> vps_person;
        vps_person["HAS_CREATOR"] = std::any();
        if (post_id != 0)
        {
            Post.getVersionInRange(ts, post_id, vps_person, eps, ves);
        }
        if (vps_person["HAS_CREATOR"].has_value())
        {
            std::unordered_map<std::string, std::any> vps_get_person;
            vps_get_person["firstName"] = std::any();
            vps_get_person["lastName"] = std::any();
            uint64_t creator_id = std::any_cast<uint64_t>(vps_person["HAS_CREATOR"]);
            Person.getVersionInRange(ts, person_id[creator_id], vps_get_person, eps, ves);
            res["firstName"] = vps_get_person["firstName"];
            res["lastName"] = vps_get_person["lastName"];
        }
    }
    std::sort(res_id.begin(), res_id.end());
    return res;
}

std::unordered_map<std::string, std::any> IS3(column::GraphDB& Person,
                                              std::unordered_map<uint64_t, uint64_t>& person_id,
                                              ctgraph::CtStore& Person_Person_KNOWS, uint64_t& person_Initial_id,
                                              uint64_t& ts)
{
    std::vector<ctgraph::Edge> edges;
    Person_Person_KNOWS.get_edges_withTs(person_id[person_Initial_id], edges, ts);
    std::unordered_map<std::string, std::any> vps;
    std::unordered_map<std::string, std::any> eps;
    std::unordered_map<std::string, std::any> ves;
    vps["firstName"] = std::any();
    vps["lastName"] = std::any();
    for (auto edge : edges)
    {
        Person.getVersionInRange(ts, edge.destination(), vps, eps, ves);
    }
    return vps;
}

std::unordered_map<std::string, std::any> IS4(column::GraphDB& Comment, column::GraphDB& Post,
                                              std::unordered_map<uint64_t, uint64_t>& comment_id,
                                              std::unordered_map<uint64_t, uint64_t>& post_id, uint64_t& Initial_id,
                                              uint64_t& ts)
{
    std::unordered_map<std::string, std::any> res;
    res["content"] = std::any();
    res["imageFile"] = std::any();
    res["creationDate"] = std::any();
    if (post_id.find(Initial_id) != post_id.end())
    {
        std::unordered_map<std::string, std::any> vps;
        std::unordered_map<std::string, std::any> eps;
        std::unordered_map<std::string, std::any> ves;
        vps["content"] = std::any();
        vps["imageFile"] = std::any();
        vps["creationDate"] = std::any();
        Post.getVersionInRange(ts, post_id[Initial_id], vps, eps, ves);
        res["content"] = vps["content"];
        res["imageFile"] = vps["imageFile"];
        res["creationDate"] = vps["creationDate"];
    }
    if (comment_id.find(Initial_id) != comment_id.end())
    {
        std::unordered_map<std::string, std::any> vps;
        std::unordered_map<std::string, std::any> eps;
        std::unordered_map<std::string, std::any> ves;
        vps["content"] = std::any();
        vps["creationDate"] = std::any();
        Comment.getVersionInRange(ts, comment_id[Initial_id], vps, eps, ves);
        res["content"] = vps["content"];
        res["creationDate"] = vps["creationDate"];
    }
    return res;
}

std::unordered_map<std::string, std::any> IS5(column::GraphDB& Person, column::GraphDB& Comment, column::GraphDB& Post,
                                              std::unordered_map<uint64_t, uint64_t>& person_id,
                                              std::unordered_map<uint64_t, uint64_t>& comment_id,
                                              std::unordered_map<uint64_t, uint64_t>& post_id, uint64_t& Initial_id,
                                              uint64_t& ts)
{
    std::unordered_map<std::string, std::any> res;
    res["firstName"] = std::any();
    res["lastName"] = std::any();
    if (post_id.find(Initial_id) != post_id.end())
    {
        std::unordered_map<std::string, std::any> vps_post;
        std::unordered_map<std::string, std::any> eps_post;
        std::unordered_map<std::string, std::any> ves_post;
        vps_post["HAS_CREATOR"] = std::any();
        Post.getVersionInRange(ts, post_id[Initial_id], vps_post, eps_post, ves_post);

        if (vps_post.find("HAS_CREATOR") != vps_post.end() && vps_post["HAS_CREATOR"].has_value())
        {
            uint64_t creator_id = std::any_cast<uint64_t>(vps_post["HAS_CREATOR"]);
            if (person_id.find(creator_id) != person_id.end())
            {
                std::unordered_map<std::string, std::any> vps_person;
                std::unordered_map<std::string, std::any> eps_person;
                std::unordered_map<std::string, std::any> ves_person;
                vps_person["firstName"] = std::any();
                vps_person["lastName"] = std::any();
                Person.getVersionInRange(ts, person_id[creator_id], vps_person, eps_person, ves_person);
                res["firstName"] = vps_person["firstName"];
                res["lastName"] = vps_person["lastName"];
            }
        }
    }

    if (comment_id.find(Initial_id) != comment_id.end())
    {
        std::unordered_map<std::string, std::any> vps_comment;
        std::unordered_map<std::string, std::any> eps_comment;
        std::unordered_map<std::string, std::any> ves_comment;
        vps_comment["HAS_CREATOR"] = std::any();
        Comment.getVersionInRange(ts, comment_id[Initial_id], vps_comment, eps_comment, ves_comment);

        if (vps_comment.find("HAS_CREATOR") != vps_comment.end() && vps_comment["HAS_CREATOR"].has_value())
        {
            uint64_t creator_id = std::any_cast<uint64_t>(vps_comment["HAS_CREATOR"]);
            if (person_id.find(creator_id) != person_id.end())
            {
                std::unordered_map<std::string, std::any> vps_person;
                std::unordered_map<std::string, std::any> eps_person;
                std::unordered_map<std::string, std::any> ves_person;
                vps_person["firstName"] = std::any();
                vps_person["lastName"] = std::any();
                Person.getVersionInRange(ts, person_id[creator_id], vps_person, eps_person, ves_person);
                res["firstName"] = vps_person["firstName"];
                res["lastName"] = vps_person["lastName"];
            }
        }
    }
    return res;
}

void IS6(column::GraphDB& Forum, column::GraphDB& Person, column::GraphDB& Comment, column::GraphDB& Post,
         std::unordered_map<uint64_t, uint64_t>& person_id, std::unordered_map<uint64_t, uint64_t>& comment_id,
         std::unordered_map<uint64_t, uint64_t>& post_id, ctgraph::CtStore& Comment_Post_REPLY_OF, uint64_t& Initial_id,
         uint64_t& ts)
{
    std::unordered_map<std::string, std::any> vps;
    std::unordered_map<std::string, std::any> eps;
    std::unordered_map<std::string, std::any> ves;
    vps["REPLY_OF_COMMENT"] = std::any();
    uint64_t comment_id_get = comment_id[Initial_id];
    Comment.getVersionInRange(ts, comment_id_get, vps, eps, ves);
    if (vps["REPLY_OF_COMMENT"].has_value())
    {
        comment_id_get = std::any_cast<uint64_t>(vps["REPLY_OF_COMMENT"]);
    }
    while (vps["REPLY_OF_COMMENT"].has_value())
    {
        vps["REPLY_OF_COMMENT"] = std::any();
        Comment.getVersionInRange(ts, comment_id_get, vps, eps, ves);
        if (vps["REPLY_OF_COMMENT"].has_value())
        {
            comment_id_get = std::any_cast<uint64_t>(vps["REPLY_OF_COMMENT"]);
        }
    }
    std::unordered_map<std::string, std::any> vps_post;
    vps_post["REPLY_OF_POST"] = std::any();
    Comment.getVersionInRange(ts, comment_id_get, vps_post, eps, ves);
    uint64_t post_id_get = 0;
    if (vps_post["REPLY_OF_POST"].has_value())
    {
        post_id_get = std::any_cast<uint64_t>(vps_post["REPLY_OF_POST"]);
    }
    std::unordered_map<std::string, std::any> vps_post2;
    vps_post2["CONTAINER_OF"] = std::any();
    Post.getVersionInRange(ts, post_id_get, vps_post2, eps, ves);
    if (vps_post2["CONTAINER_OF"].has_value())
    {
        uint64_t forum_id = std::any_cast<uint64_t>(vps_post2["CONTAINER_OF"]);
        std::unordered_map<std::string, std::any> vps2;
        std::unordered_map<std::string, std::any> eps2;
        std::unordered_map<std::string, std::any> ves2;
        vps2["HAS_MODERATOR"] = std::any();
        vps2["title"] = std::any();
        vps2["creationDate"] = std::any();
        Forum.getVersionInRange(ts, forum_id, vps2, eps2, ves2);
        if (vps2["HAS_MODERATOR"].has_value())
        {
            uint64_t moderator_id = std::any_cast<uint64_t>(vps2["HAS_MODERATOR"]);
            std::unordered_map<std::string, std::any> vps3;
            std::unordered_map<std::string, std::any> eps3;
            std::unordered_map<std::string, std::any> ves3;
            vps3["firstName"] = std::any();
            vps3["lastName"] = std::any();
            Person.getVersionInRange(ts, moderator_id, vps3, eps3, ves3);
            Person.getLatestVP(moderator_id, {"firstName", "lastName"}, vps3);
        }
    }
}

void IS7(column::GraphDB& Person, column::GraphDB& Comment, column::GraphDB& Post,
         std::unordered_map<uint64_t, uint64_t>& person_id, std::unordered_map<uint64_t, uint64_t>& comment_id,
         std::unordered_map<uint64_t, uint64_t>& post_id, ctgraph::CtStore& Comment_Comment_REPLY_OF,
         ctgraph::CtStore& Person_Person_KNOWS, uint64_t& Initial_id, uint64_t& ts)
{
    bool flag = false;
    if (comment_id.find(Initial_id) != comment_id.end())
    {
        std::vector<ctgraph::Edge> edges;
        Comment_Comment_REPLY_OF.get_edges_withTs(comment_id[Initial_id], edges, ts);
        for (auto edge : edges)
        {
            std::unordered_map<std::string, std::any> vps;
            std::unordered_map<std::string, std::any> eps;
            std::unordered_map<std::string, std::any> ves;
            vps["HAS_CREATOR"] = std::any();
            Comment.getVersionInRange(ts, edge.destination(), vps, eps, ves);
            if (vps["HAS_CREATOR"].has_value())
            {
                uint64_t moderator_id = std::any_cast<uint64_t>(vps["HAS_CREATOR"]);
                std::unordered_map<std::string, std::any> vps2;
                std::unordered_map<std::string, std::any> eps2;
                std::unordered_map<std::string, std::any> ves2;
                vps2["HAS_CREATOR"] = std::any();
                Comment.getVersionInRange(ts, comment_id[Initial_id], vps2, eps2, ves2);
                if (vps2["HAS_CREATOR"].has_value())
                {
                    uint64_t creator_id = std::any_cast<uint64_t>(vps2["HAS_CREATOR"]);
                    std::vector<ctgraph::Edge> edges2;
                    Person_Person_KNOWS.get_edges_withTs(person_id[moderator_id], edges2, ts);
                    for (auto edge2 : edges2)
                    {
                        if (edge2.destination() == person_id[creator_id])
                        {
                            flag = true;
                            std::unordered_map<std::string, std::any> vps3;
                            std::unordered_map<std::string, std::any> eps3;
                            std::unordered_map<std::string, std::any> ves3;
                            vps3["creationDate"] = std::any();
                            Comment.getVersionInRange(ts, edge.destination(), vps3, eps3, ves3);
                            break;
                        }
                    }
                }
            }
        }
    }
}

int main(int argc, char** argv)
{
    // 获取数据集方式AeonG跑tests/experiments/t_mgBench_test.sh
    // lwh@LAPTOP-O6PFFAVH:~/dataset$ sudo cp
    // /mnt/d/code/AeonG/tests/datasets/T-mgBench/small.cypher
    // /workspace/dataset/small/mybench/ sudo cp
    // /mnt/d/code/AeonG/tests/results/graph_op/cypher.txt
    // /workspace/dataset/small/mybench/ sudo cp
    // /mnt/d/code/AeonG/tests/results/temporal_query/cypher_Q*
    // /workspace/dataset/small/mybench/
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    std::cout << "reserve node num of hashtable" << FLAGS_reserve_node << std::endl;
    std::cout << "ldbc line num" << FLAGS_ldbc_line_num << std::endl;

    // Establish the correspondence between num and vertex_id
    std::string num = "40"; // default
    ctgraph::VertexId_t vertex_id = 15000;

    // 根据num设置vertex_id
    if (num == "8")
    {
        vertex_id = 15000;
    }
    else if (num == "16")
    {
        vertex_id = 18000;
    }
    else if (num == "24")
    {
        vertex_id = 22000;
    }
    else if (num == "32")
    {
        vertex_id = 26000;
    }
    else if (num == "40")
    {
        vertex_id = 30000;
    }

    std::cout << "num=" << num << ", vertex_id=" << vertex_id << std::endl;

    std::atomic<ctgraph::VertexId_t> ver = vertex_id;
    ctgraph::CtStore ct_store(ver);
    column::GraphDB db;

    column::GraphDB Forum;
    column::GraphDB Comment;
    column::GraphDB Post;
    column::GraphDB Organisation;
    column::GraphDB Person;
    column::GraphDB Place;
    column::GraphDB Tag;
    column::GraphDB TagClass;

    ctgraph::VertexId_t VComment_Tag_HAS_TAG = 2052190;
    std::atomic<ctgraph::VertexId_t> ver_Comment_Comment_RePLY_OF = VComment_Tag_HAS_TAG;
    ctgraph::CtStore Comment_Tag_HAS_TAG(ver_Comment_Comment_RePLY_OF);
    for (int i = 0; i < VComment_Tag_HAS_TAG; i++)
    {
        Comment_Tag_HAS_TAG.load_vertex(i);
    }

    ctgraph::VertexId_t VComment_Post_REPLY_OF = 2052190;
    std::atomic<ctgraph::VertexId_t> ver_Comment_Post_REPLY_OF = VComment_Post_REPLY_OF;
    ctgraph::CtStore Comment_Post_REPLY_OF(ver_Comment_Post_REPLY_OF);
    for (int i = 0; i < VComment_Post_REPLY_OF; i++)
    {
        Comment_Post_REPLY_OF.load_vertex(i);
    }

    ctgraph::VertexId_t VComment_Comment_REPLY_OF = 2052190;
    std::atomic<ctgraph::VertexId_t> ver_Comment_Comment_REPLY_OF = VComment_Comment_REPLY_OF;
    ctgraph::CtStore Comment_Comment_REPLY_OF(ver_Comment_Comment_REPLY_OF);
    for (int i = 0; i < VComment_Comment_REPLY_OF; i++)
    {
        Comment_Comment_REPLY_OF.load_vertex(i);
    }

    ctgraph::VertexId_t VPerson_Comment_HasCreator = 10000;
    std::atomic<ctgraph::VertexId_t> ver_Person_Comment_HasCreator = VPerson_Comment_HasCreator;
    ctgraph::CtStore Person_Comment_HasCreator(ver_Person_Comment_HasCreator);
    for (int i = 0; i < VPerson_Comment_HasCreator; i++)
    {
        Person_Comment_HasCreator.load_vertex(i);
    }

    ctgraph::VertexId_t VForum_Tag_HAS_TAG = 90500;
    std::atomic<ctgraph::VertexId_t> ver_Forum_Tag_HAS_TAG = VForum_Tag_HAS_TAG;
    ctgraph::CtStore Forum_Tag_HAS_TAG(ver_Forum_Tag_HAS_TAG);
    for (int i = 0; i < VForum_Tag_HAS_TAG; i++)
    {
        Forum_Tag_HAS_TAG.load_vertex(i);
    }

    ctgraph::VertexId_t VPerson_Tag_HAS_INTEREST = 10000;
    std::atomic<ctgraph::VertexId_t> ver_Person_Tag_HAS_INTEREST = VPerson_Tag_HAS_INTEREST;
    ctgraph::CtStore Person_Tag_HAS_INTEREST(ver_Person_Tag_HAS_INTEREST);
    for (int i = 0; i < VPerson_Tag_HAS_INTEREST; i++)
    {
        Person_Tag_HAS_INTEREST.load_vertex(i);
    }

    ctgraph::VertexId_t VPerson_Person_KNOWS = 10000;
    std::atomic<ctgraph::VertexId_t> ver_Person_Person_KNOWS = VPerson_Person_KNOWS;
    ctgraph::CtStore Person_Person_KNOWS(ver_Person_Person_KNOWS);
    for (int i = 0; i < VPerson_Person_KNOWS; i++)
    {
        Person_Person_KNOWS.load_vertex(i);
    }

    ctgraph::VertexId_t VPerson_Comment_LIKES = 10000;
    std::atomic<ctgraph::VertexId_t> ver_Person_Comment_LIKES = VPerson_Comment_LIKES;
    ctgraph::CtStore Person_Comment_LIKES(ver_Person_Comment_LIKES);
    for (int i = 0; i < VPerson_Comment_LIKES; i++)
    {
        Person_Comment_LIKES.load_vertex(i);
    }

    ctgraph::VertexId_t VPerson_Post_LIKES = 10000;
    std::atomic<ctgraph::VertexId_t> ver_Person_Post_LIKES = VPerson_Post_LIKES;
    ctgraph::CtStore Person_Post_LIKES(ver_Person_Post_LIKES);
    for (int i = 0; i < VPerson_Post_LIKES; i++)
    {
        Person_Post_LIKES.load_vertex(i);
    }

    ctgraph::VertexId_t VPerson_Organisation_WORK_AT = 10000;
    std::atomic<ctgraph::VertexId_t> ver_Person_Organisation_WORK_AT = VPerson_Organisation_WORK_AT;
    ctgraph::CtStore Person_Organisation_WORK_AT(ver_Person_Organisation_WORK_AT);
    for (int i = 0; i < VPerson_Organisation_WORK_AT; i++)
    {
        Person_Organisation_WORK_AT.load_vertex(i);
    }

    ctgraph::VertexId_t VPost_Tag_HAS_TAG = 1003700;
    std::atomic<ctgraph::VertexId_t> ver_Post_Tag_HAS_TAG = VPost_Tag_HAS_TAG;
    ctgraph::CtStore Post_Tag_HAS_TAG(ver_Post_Tag_HAS_TAG);
    for (int i = 0; i < VPost_Tag_HAS_TAG; i++)
    {
        Post_Tag_HAS_TAG.load_vertex(i);
    }
    std::unordered_map<uint64_t, uint64_t> comment_id;
    std::unordered_map<uint64_t, uint64_t> post_id;
    std::unordered_map<uint64_t, uint64_t> person_id;
    std::unordered_map<uint64_t, uint64_t> forum_id;
    std::cout << "begin load data" << std::endl;
    struct rusage usage_before;
    getrusage(RUSAGE_SELF, &usage_before);
    long start_memory = usage_before.ru_maxrss;

    auto start_time = std::chrono::high_resolution_clock::now();

    // Initial_organisation
    std::cout << "begin load Organisation..." << std::endl;
    struct rusage usage_before_org;
    getrusage(RUSAGE_SELF, &usage_before_org);
    long start_memory_org = usage_before_org.ru_maxrss;
    auto start_time_org = std::chrono::high_resolution_clock::now();

    Initial_organisation(Organisation);

    auto end_time_org = std::chrono::high_resolution_clock::now();
    struct rusage usage_after_org;
    getrusage(RUSAGE_SELF, &usage_after_org);
    long end_memory_org = usage_after_org.ru_maxrss;
    auto duration_org = std::chrono::duration_cast<std::chrono::microseconds>(end_time_org - start_time_org);
    std::cout << "Organisation load time: " << duration_org.count() << " us" << std::endl;
    std::cout << "Organisation memory cost: " << (end_memory_org - start_memory_org) / 1024.0 << " MB" << std::endl;

    // Initial_place
    std::cout << "begin load Place..." << std::endl;
    struct rusage usage_before_place;
    getrusage(RUSAGE_SELF, &usage_before_place);
    long start_memory_place = usage_before_place.ru_maxrss;
    auto start_time_place = std::chrono::high_resolution_clock::now();

    Initial_place(Place);

    auto end_time_place = std::chrono::high_resolution_clock::now();
    struct rusage usage_after_place;
    getrusage(RUSAGE_SELF, &usage_after_place);
    long end_memory_place = usage_after_place.ru_maxrss;
    auto duration_place = std::chrono::duration_cast<std::chrono::microseconds>(end_time_place - start_time_place);
    std::cout << "Placeload time: " << duration_place.count() << " us" << std::endl;
    std::cout << "Placememory cost: " << (end_memory_place - start_memory_place) / 1024.0 << " MB" << std::endl;

    // Initial_tag
    std::cout << "begin load Tag..." << std::endl;
    struct rusage usage_before_tag;
    getrusage(RUSAGE_SELF, &usage_before_tag);
    long start_memory_tag = usage_before_tag.ru_maxrss;
    auto start_time_tag = std::chrono::high_resolution_clock::now();

    Initial_tag(Tag);

    auto end_time_tag = std::chrono::high_resolution_clock::now();
    struct rusage usage_after_tag;
    getrusage(RUSAGE_SELF, &usage_after_tag);
    long end_memory_tag = usage_after_tag.ru_maxrss;
    auto duration_tag = std::chrono::duration_cast<std::chrono::microseconds>(end_time_tag - start_time_tag);
    std::cout << "Tagload time: " << duration_tag.count() << " us" << std::endl;
    std::cout << "Tagmemory cost: " << (end_memory_tag - start_memory_tag) / 1024.0 << " MB" << std::endl;

    // Initial_tagclass
    std::cout << "begin load TagClass..." << std::endl;
    struct rusage usage_before_tagclass;
    getrusage(RUSAGE_SELF, &usage_before_tagclass);
    long start_memory_tagclass = usage_before_tagclass.ru_maxrss;
    auto start_time_tagclass = std::chrono::high_resolution_clock::now();

    Initial_tagclass(TagClass);

    auto end_time_tagclass = std::chrono::high_resolution_clock::now();
    struct rusage usage_after_tagclass;
    getrusage(RUSAGE_SELF, &usage_after_tagclass);
    long end_memory_tagclass = usage_after_tagclass.ru_maxrss;
    auto duration_tagclass =
        std::chrono::duration_cast<std::chrono::microseconds>(end_time_tagclass - start_time_tagclass);
    std::cout << "TagClassload time: " << duration_tagclass.count() << " us" << std::endl;
    std::cout << "TagClassmemory cost: " << (end_memory_tagclass - start_memory_tagclass) / 1024.0 << " MB"
              << std::endl;

    // Initial_comment_post
    std::cout << "begin load Comment,Post..." << std::endl;
    struct rusage usage_before_cp;
    getrusage(RUSAGE_SELF, &usage_before_cp);
    long start_memory_cp = usage_before_cp.ru_maxrss;
    auto start_time_cp = std::chrono::high_resolution_clock::now();

    Initial_comment_post(Comment, Post, Person, comment_id, post_id, person_id, Comment_Tag_HAS_TAG,
                         Comment_Post_REPLY_OF, Comment_Comment_REPLY_OF, Person_Comment_HasCreator);

    auto end_time_cp = std::chrono::high_resolution_clock::now();
    struct rusage usage_after_cp;
    getrusage(RUSAGE_SELF, &usage_after_cp);
    long end_memory_cp = usage_after_cp.ru_maxrss;
    auto duration_cp = std::chrono::duration_cast<std::chrono::microseconds>(end_time_cp - start_time_cp);
    std::cout << "Comment和Postload time: " << duration_cp.count() << " us" << std::endl;
    std::cout << "Comment和Postmemory cost: " << (end_memory_cp - start_memory_cp) / 1024.0 << " MB" << std::endl;

    // Initial_forum
    std::cout << "begin load Forum..." << std::endl;
    struct rusage usage_before_forum;
    getrusage(RUSAGE_SELF, &usage_before_forum);
    long start_memory_forum = usage_before_forum.ru_maxrss;
    auto start_time_forum = std::chrono::high_resolution_clock::now();

    Initial_forum(Forum, Post, forum_id, post_id, Forum_Tag_HAS_TAG);

    auto end_time_forum = std::chrono::high_resolution_clock::now();
    struct rusage usage_after_forum;
    getrusage(RUSAGE_SELF, &usage_after_forum);
    long end_memory_forum = usage_after_forum.ru_maxrss;
    auto duration_forum = std::chrono::duration_cast<std::chrono::microseconds>(end_time_forum - start_time_forum);
    std::cout << "Forumload time: " << duration_forum.count() << " us" << std::endl;
    std::cout << "Forummemory cost: " << (end_memory_forum - start_memory_forum) / 1024.0 << " MB" << std::endl;

    // Intial_Person
    std::cout << "begin load Person..." << std::endl;
    struct rusage usage_before_person;
    getrusage(RUSAGE_SELF, &usage_before_person);
    long start_memory_person = usage_before_person.ru_maxrss;
    auto start_time_person = std::chrono::high_resolution_clock::now();

    Intial_Person(Person, person_id, comment_id, post_id, Person_Tag_HAS_INTEREST, Person_Person_KNOWS,
                  Person_Comment_LIKES, Person_Post_LIKES, Person_Organisation_WORK_AT);

    auto end_time_person = std::chrono::high_resolution_clock::now();
    struct rusage usage_after_person;
    getrusage(RUSAGE_SELF, &usage_after_person);
    long end_memory_person = usage_after_person.ru_maxrss;
    auto duration_person = std::chrono::duration_cast<std::chrono::microseconds>(end_time_person - start_time_person);
    std::cout << "Person load time: " << duration_person.count() << " us" << std::endl;
    std::cout << "Person memory cost: " << (end_memory_person - start_memory_person) / 1024.0 << " MB" << std::endl;

    // Initial_Post
    std::cout << "begin load Post..." << std::endl;
    struct rusage usage_before_post;
    getrusage(RUSAGE_SELF, &usage_before_post);
    long start_memory_post = usage_before_post.ru_maxrss;
    auto start_time_post = std::chrono::high_resolution_clock::now();

    Initial_Post(Post, post_id, Post_Tag_HAS_TAG);

    auto end_time_post = std::chrono::high_resolution_clock::now();
    struct rusage usage_after_post;
    getrusage(RUSAGE_SELF, &usage_after_post);
    long end_memory_post = usage_after_post.ru_maxrss;
    auto duration_post = std::chrono::duration_cast<std::chrono::microseconds>(end_time_post - start_time_post);
    std::cout << "Post load time: " << duration_post.count() << " us" << std::endl;
    std::cout << "Post memory cost: " << (end_memory_post - start_memory_post) / 1024.0 << " MB" << std::endl;

    auto end_time = std::chrono::high_resolution_clock::now();
    struct rusage usage_after;
    getrusage(RUSAGE_SELF, &usage_after);
    long end_memory = usage_after.ru_maxrss;

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    std::cout << "All load time " << duration.count() << " us" << std::endl;
    std::cout << "All mem cost: " << (end_memory - start_memory) / 1024.0 << " MB" << std::endl;

    std::cout << "begin update data" << std::endl;
    struct rusage usage_before_update;
    getrusage(RUSAGE_SELF, &usage_before_update);
    long start_memory_update = usage_before_update.ru_maxrss;

    auto start_time_update = std::chrono::high_resolution_clock::now();
    Update(Person, Comment, person_id, comment_id);
    auto end_time_update = std::chrono::high_resolution_clock::now();

    struct rusage usage_after_update;
    getrusage(RUSAGE_SELF, &usage_after_update);
    long end_memory_update = usage_after_update.ru_maxrss;

    auto duration_update = std::chrono::duration_cast<std::chrono::microseconds>(end_time_update - start_time_update);
    std::cout << "update time: " << duration_update.count() << " us" << std::endl;
    std::cout << "update cost mem: " << (end_memory_update - start_memory_update) / 1024.0 << " MB" << std::endl;

    //------------------IS1---------------
    std::cout << "IS1" << std::endl;

    struct rusage usage_before_is1;
    getrusage(RUSAGE_SELF, &usage_before_is1);
    long start_memory_is1 = usage_before_is1.ru_maxrss;
    auto start_time_is1 = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<uint64_t, uint64_t>> is1_queries;
    std::ifstream is1_file("/workspace/dataset/ldbc/temporal_query/IS1_cypher.txt");
    std::string is1_line;

    if (!is1_file.is_open())
    {
        std::cerr << "cannot open IS1_cypher.txt" << std::endl;
    }
    else
    {
        while (std::getline(is1_file, is1_line))
        {
            // person id
            size_t id_start = is1_line.find("{id:") + 4;
            size_t id_end = is1_line.find("}", id_start);
            uint64_t person_Initial_id = std::stoull(is1_line.substr(id_start, id_end - id_start));

            // ts
            size_t ts_start = is1_line.find("TT AS ") + 6;
            size_t ts_end = is1_line.find(" ", ts_start);
            uint64_t ts = std::stoull(is1_line.substr(ts_start, ts_end - ts_start)) % FLAGS_ldbc_line_num;

            is1_queries.push_back({person_Initial_id, ts});
        }
        is1_file.close();
    }

    if (!is1_queries.empty())
    {
        for (const auto& query : is1_queries)
        {
            uint64_t person_Initial_id = query.first;
            uint64_t ts = query.second;
            IS1(Person, Place, person_id, person_Initial_id, ts);
        }

        auto end_time_is1 = std::chrono::high_resolution_clock::now();
        struct rusage usage_after_is1;
        getrusage(RUSAGE_SELF, &usage_after_is1);
        long end_memory_is1 = usage_after_is1.ru_maxrss;
        auto duration_is1 = std::chrono::duration_cast<std::chrono::microseconds>(end_time_is1 - start_time_is1);

        std::cout << "IS1all query num: " << is1_queries.size() << std::endl;
        std::cout << "IS1 query time: " << duration_is1.count() << " us" << std::endl;
        std::cout << "IS1 avg query time: " << (double)duration_is1.count() / is1_queries.size() << " us" << std::endl;
        // for it caculate twice
        std::cout << "IS1 mem cost: " << (end_memory_is1 - start_memory_is1) / 1024.0 << " MB" << std::endl;
    }
    else
    {
        std::cout << "IS1 has no query" << std::endl;
    }

    //------------------IS2---------------
    std::cout << "IS2" << std::endl;

    struct rusage usage_before_is2;
    getrusage(RUSAGE_SELF, &usage_before_is2);
    long start_memory_is2 = usage_before_is2.ru_maxrss;
    auto start_time_is2 = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<uint64_t, uint64_t>> is2_queries;
    std::ifstream is2_file("/workspace/dataset/ldbc/temporal_query/IS2_cypher.txt");
    std::string is2_line;

    if (!is2_file.is_open())
    {
        std::cerr << "cannot open IS2_cypher.txt文件" << std::endl;
    }
    else
    {
        while (std::getline(is2_file, is2_line))
        {
            // person id
            size_t id_start = is2_line.find("{id:") + 4;
            size_t id_end = is2_line.find("}", id_start);
            uint64_t person_Initial_id = std::stoull(is2_line.substr(id_start, id_end - id_start));

            // ts
            size_t ts_start = is2_line.find("TT AS ") + 6;
            size_t ts_end = is2_line.find(" ", ts_start);
            uint64_t ts = std::stoull(is2_line.substr(ts_start, ts_end - ts_start)) % FLAGS_ldbc_line_num;

            is2_queries.push_back({person_Initial_id, ts});
        }
        is2_file.close();
    }

    if (!is2_queries.empty())
    {
        for (const auto& query : is2_queries)
        {
            uint64_t person_Initial_id = query.first;
            uint64_t ts = query.second;
            IS2(Person, Comment, Post, person_id, comment_id, post_id, Person_Comment_HasCreator, person_Initial_id, ts,
                Comment_Post_REPLY_OF);
        }

        auto end_time_is2 = std::chrono::high_resolution_clock::now();
        struct rusage usage_after_is2;
        getrusage(RUSAGE_SELF, &usage_after_is2);
        long end_memory_is2 = usage_after_is2.ru_maxrss;
        auto duration_is2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time_is2 - start_time_is2);

        std::cout << "IS2all query num: " << is2_queries.size() << std::endl;
        std::cout << "IS2 query time: " << duration_is2.count() << " us" << std::endl;
        std::cout << "IS2avg query time: " << (double)duration_is2.count() / is2_queries.size() << " us" << std::endl;
        std::cout << "IS2 mem cost: " << (end_memory_is2 - start_memory_is2) / 1024.0 << " MB" << std::endl;
    }
    else
    {
        std::cout << "IS2have no query" << std::endl;
    }

    //------------------IS3---------------
    std::cout << "IS3" << std::endl;

    struct rusage usage_before_is3;
    getrusage(RUSAGE_SELF, &usage_before_is3);
    long start_memory_is3 = usage_before_is3.ru_maxrss;
    auto start_time_is3 = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<uint64_t, uint64_t>> is3_queries;
    std::ifstream is3_file("/workspace/dataset/ldbc/temporal_query/IS3_cypher.txt");
    std::string is3_line;

    if (!is3_file.is_open())
    {
        std::cerr << "cannot open IS3_cypher.txt文件" << std::endl;
    }
    else
    {
        while (std::getline(is3_file, is3_line))
        {
            if (is3_line.find("MATCH (n:Person {id:") != std::string::npos)
            {
                size_t id_start = is3_line.find("{id:") + 4;
                size_t id_end = is3_line.find("}", id_start);
                uint64_t person_Initial_id = std::stoull(is3_line.substr(id_start, id_end - id_start));
                if (std::getline(is3_file, is3_line) && is3_line.find("TT AS") != std::string::npos)
                {
                    // ts
                    size_t ts_start = is3_line.find("TT AS ") + 6;
                    size_t ts_end = is3_line.find(" ", ts_start);
                    uint64_t ts = std::stoull(is3_line.substr(ts_start, ts_end - ts_start)) % FLAGS_ldbc_line_num;

                    is3_queries.push_back({person_Initial_id, ts});
                    for (int i = 0; i < 6; i++)
                    {
                        std::getline(is3_file, is3_line);
                    }
                }
            }
        }
        is3_file.close();
    }

    if (!is3_queries.empty())
    {
        for (const auto& query : is3_queries)
        {
            uint64_t person_Initial_id = query.first;
            uint64_t ts = query.second;
            IS3(Person, person_id, Person_Person_KNOWS, person_Initial_id, ts);
        }

        auto end_time_is3 = std::chrono::high_resolution_clock::now();
        struct rusage usage_after_is3;
        getrusage(RUSAGE_SELF, &usage_after_is3);
        long end_memory_is3 = usage_after_is3.ru_maxrss;
        auto duration_is3 = std::chrono::duration_cast<std::chrono::microseconds>(end_time_is3 - start_time_is3);

        std::cout << "IS3all query num: " << is3_queries.size() << std::endl;
        std::cout << "IS3 query time: " << duration_is3.count() << " us" << std::endl;
        std::cout << "IS3avg query time: " << (double)duration_is3.count() / is3_queries.size() << " us" << std::endl;
        std::cout << "IS3 mem cost: " << (end_memory_is3 - start_memory_is3) / 1024.0 << " MB" << std::endl;
    }
    else
    {
        std::cout << "IS3have no query" << std::endl;
    }

    //------------------IS4---------------
    std::cout << "IS4" << std::endl;


    struct rusage usage_before_is4;
    getrusage(RUSAGE_SELF, &usage_before_is4);
    long start_memory_is4 = usage_before_is4.ru_maxrss;
    auto start_time_is4 = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<uint64_t, uint64_t>> is4_queries;
    std::ifstream is4_file("/workspace/dataset/ldbc/temporal_query/IS4_cypher.txt");
    std::string is4_line;

    if (!is4_file.is_open())
    {
        std::cerr << "cannot open IS4_cypher.txt文件" << std::endl;
    }
    else
    {
        while (std::getline(is4_file, is4_line))
        {
            size_t id_start = is4_line.find("{id:") + 4;
            size_t id_end = is4_line.find("}", id_start);
            uint64_t message_id = std::stoull(is4_line.substr(id_start, id_end - id_start));

            // ts
            size_t ts_start = is4_line.find("TT AS ") + 6;
            size_t ts_end = is4_line.find(" ", ts_start);
            uint64_t ts = std::stoull(is4_line.substr(ts_start, ts_end - ts_start)) % FLAGS_ldbc_line_num;

            is4_queries.push_back({message_id, ts});
        }
        is4_file.close();
    }

    if (!is4_queries.empty())
    {
        for (const auto& query : is4_queries)
        {
            uint64_t message_id = query.first;
            uint64_t ts = query.second;
            IS4(Comment, Post, comment_id, post_id, message_id, ts);
        }

        auto end_time_is4 = std::chrono::high_resolution_clock::now();
        struct rusage usage_after_is4;
        getrusage(RUSAGE_SELF, &usage_after_is4);
        long end_memory_is4 = usage_after_is4.ru_maxrss;
        auto duration_is4 = std::chrono::duration_cast<std::chrono::microseconds>(end_time_is4 - start_time_is4);

        std::cout << "IS4all query num: " << is4_queries.size() << std::endl;
        std::cout << "IS4 query time: " << duration_is4.count() << " us" << std::endl;
        std::cout << "IS4avg query time: " << (double)duration_is4.count() / is4_queries.size() << " us" << std::endl;
        std::cout << "IS4 mem cost: " << (end_memory_is4 - start_memory_is4) / 1024.0 << " MB" << std::endl;
    }
    else
    {
        std::cout << "IS4have no query" << std::endl;
    }

    //------------------IS5---------------
    std::cout << "IS5" << std::endl;


    struct rusage usage_before_is5;
    getrusage(RUSAGE_SELF, &usage_before_is5);
    long start_memory_is5 = usage_before_is5.ru_maxrss;
    auto start_time_is5 = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<uint64_t, uint64_t>> is5_queries;
    std::ifstream is5_file("/workspace/dataset/ldbc/temporal_query/IS5_cypher.txt");
    std::string is5_line;

    if (!is5_file.is_open())
    {
        std::cerr << "cannot open IS5_cypher.txt文件" << std::endl;
    }
    else
    {
        while (std::getline(is5_file, is5_line))
        {
            size_t id_start = is5_line.find("{id:") + 4;
            size_t id_end = is5_line.find("}", id_start);
            uint64_t message_id = std::stoull(is5_line.substr(id_start, id_end - id_start));

            // ts
            size_t ts_start = is5_line.find("TT AS ") + 6;
            size_t ts_end = is5_line.find(" ", ts_start);
            uint64_t ts = std::stoull(is5_line.substr(ts_start, ts_end - ts_start)) % FLAGS_ldbc_line_num;

            is5_queries.push_back({message_id, ts});
        }
        is5_file.close();
    }

    if (!is5_queries.empty())
    {
        for (const auto& query : is5_queries)
        {
            uint64_t message_id = query.first;
            uint64_t ts = query.second;
            IS5(Person, Comment, Post, person_id, comment_id, post_id, message_id, ts);
        }

        auto end_time_is5 = std::chrono::high_resolution_clock::now();
        struct rusage usage_after_is5;
        getrusage(RUSAGE_SELF, &usage_after_is5);
        long end_memory_is5 = usage_after_is5.ru_maxrss;
        auto duration_is5 = std::chrono::duration_cast<std::chrono::microseconds>(end_time_is5 - start_time_is5);

        std::cout << "IS5all query num: " << is5_queries.size() << std::endl;
        std::cout << "IS5 query time: " << duration_is5.count() << " us" << std::endl;
        std::cout << "IS5avg query time: " << (double)duration_is5.count() / is5_queries.size() << " us" << std::endl;
        std::cout << "IS5 mem cost: " << (end_memory_is5 - start_memory_is5) / 1024.0 << " MB" << std::endl;
    }
    else
    {
        std::cout << "IS5have no query" << std::endl;
    }

    //------------------IS6---------------
    std::cout << "IS6" << std::endl;


    struct rusage usage_before_is6;
    getrusage(RUSAGE_SELF, &usage_before_is6);
    long start_memory_is6 = usage_before_is6.ru_maxrss;
    auto start_time_is6 = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<uint64_t, uint64_t>> is6_queries;
    std::ifstream is6_file("/workspace/dataset/ldbc/temporal_query/IS6_cypher.txt");
    std::string is6_line;

    if (!is6_file.is_open())
    {
        std::cerr << "cannot open IS6_cypher.txt文件" << std::endl;
    }
    else
    {
        while (std::getline(is6_file, is6_line))
        {
            size_t id_start = is6_line.find("{id:") + 4;
            size_t id_end = is6_line.find("}", id_start);
            uint64_t person_Initial_id = std::stoull(is6_line.substr(id_start, id_end - id_start));
            // ts
            size_t ts_start = is6_line.find("TT AS ") + 6;
            size_t ts_end = is6_line.find(" ", ts_start);
            uint64_t ts = std::stoull(is6_line.substr(ts_start, ts_end - ts_start)) % FLAGS_ldbc_line_num;

            is6_queries.push_back({person_Initial_id, ts});
        }
        is6_file.close();
    }

    if (!is6_queries.empty())
    {
        for (const auto& query : is6_queries)
        {
            uint64_t person_Initial_id = query.first;
            uint64_t ts = query.second;
            IS6(Forum, Person, Comment, Post, person_id, comment_id, post_id, Comment_Post_REPLY_OF, person_Initial_id,
                ts);
        }

        auto end_time_is6 = std::chrono::high_resolution_clock::now();
        struct rusage usage_after_is6;
        getrusage(RUSAGE_SELF, &usage_after_is6);
        long end_memory_is6 = usage_after_is6.ru_maxrss;
        auto duration_is6 = std::chrono::duration_cast<std::chrono::microseconds>(end_time_is6 - start_time_is6);

        std::cout << "IS6all query num: " << is6_queries.size() << std::endl;
        std::cout << "IS6 query time: " << duration_is6.count() << " us" << std::endl;
        std::cout << "IS6avg query time: " << (double)duration_is6.count() / is6_queries.size() << " us" << std::endl;
        std::cout << "IS6 mem cost: " << (end_memory_is6 - start_memory_is6) / 1024.0 << " MB" << std::endl;
    }
    else
    {
        std::cout << "IS6have no query" << std::endl;
    }

    //------------------IS7---------------
    std::cout << "IS7" << std::endl;


    struct rusage usage_before_is7;
    getrusage(RUSAGE_SELF, &usage_before_is7);
    long start_memory_is7 = usage_before_is7.ru_maxrss;
    auto start_time_is7 = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<uint64_t, uint64_t>> is7_queries;
    std::ifstream is7_file("/workspace/dataset/ldbc/temporal_query/IS7_cypher.txt");
    std::string is7_line;

    if (!is7_file.is_open())
    {
        std::cerr << "cannot open IS7_cypher.txt文件" << std::endl;
    }
    else
    {
        while (std::getline(is7_file, is7_line))
        {
            // person id
            size_t id_start = is7_line.find("{id:") + 4;
            size_t id_end = is7_line.find("}", id_start);
            uint64_t person_Initial_id = std::stoull(is7_line.substr(id_start, id_end - id_start));

            // ts
            size_t ts_start = is7_line.find("TT AS ") + 6;
            size_t ts_end = is7_line.find(" ", ts_start);
            uint64_t ts = std::stoull(is7_line.substr(ts_start, ts_end - ts_start)) % FLAGS_ldbc_line_num;

            is7_queries.push_back({person_Initial_id, ts});
        }
        is7_file.close();
    }

    if (!is7_queries.empty())
    {
        for (const auto& query : is7_queries)
        {
            uint64_t person_Initial_id = query.first;
            uint64_t ts = query.second;
            IS7(Person, Comment, Post, person_id, comment_id, post_id, Comment_Comment_REPLY_OF, Person_Person_KNOWS,
                person_Initial_id, ts);
        }

        auto end_time_is7 = std::chrono::high_resolution_clock::now();
        struct rusage usage_after_is7;
        getrusage(RUSAGE_SELF, &usage_after_is7);
        long end_memory_is7 = usage_after_is7.ru_maxrss;
        auto duration_is7 = std::chrono::duration_cast<std::chrono::microseconds>(end_time_is7 - start_time_is7);

        std::cout << "IS7all query num: " << is7_queries.size() << std::endl;
        std::cout << "IS7 query time: " << duration_is7.count() << " us" << std::endl;
        std::cout << "IS7avg query time: " << (double)duration_is7.count() / is7_queries.size() << " us" << std::endl;
        std::cout << "IS7 mem cost: " << (end_memory_is7 - start_memory_is7) / 1024.0 << " MB" << std::endl;
    }
    else
    {
        std::cout << "IS7have no query" << std::endl;
    }

    return 0;
}
